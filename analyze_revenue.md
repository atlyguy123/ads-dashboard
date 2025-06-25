# Revenue Analysis - Database Value Estimation Validation

## Mission Statement
Verify that the revenue/refund logic is working correctly for estimated values in the database. Every user who generated revenue and hasn't refunded should have a positive estimated value, and every user who refunded should have zero estimated value.

## Core Rules to Validate
- **Rule 1**: Users with revenue events ("RC Initial purchase" OR "RC Trial converted" with positive revenue) AND no subsequent "RC Cancellation" with negative revenue → current_value should be > 0
- **Rule 2**: Users with revenue events AND subsequent "RC Cancellation" with negative revenue → current_value should be = 0

---

## Phase 1: Database Discovery & Current State
**Status**: 🔄 IN PROGRESS

### Questions to Answer:
- [ ] How many total users are in `user_product_metrics`?
- [ ] What's the distribution of `current_value` (how many have 0 vs > 0)?
- [ ] How many revenue events vs refund events exist?
- [ ] Are there any obvious data quality issues?

### Tasks:
- [ ] Get overview statistics of the database
- [ ] Understand the current state of estimated values
- [ ] Count revenue-generating vs refund events

### Findings:
*To be filled as analysis progresses*

---

## Phase 2: Identify User Revenue Patterns
**Status**: ⏳ PENDING

### Questions to Answer:
- [ ] Which users have revenue-generating events ("RC Initial purchase" or "RC Trial converted" with positive revenue)?
- [ ] Which users have refund events ("RC Cancellation" with negative revenue)?
- [ ] What are the different user patterns (Revenue-only, Revenue+Refund, etc.)?

### Tasks:
- [ ] Query all users with positive revenue events
- [ ] Query all users with negative revenue (refund) events  
- [ ] Categorize users into groups:
  - **Group A**: Revenue-only (no refunds) → should have current_value > 0
  - **Group B**: Revenue + Refund → should have current_value = 0
  - **Group C**: Other edge cases

### Findings:
*To be filled as analysis progresses*

---

## Phase 3: Rule Validation
**Status**: ⏳ PENDING

### Questions to Answer:
- [ ] Do all Group A users have current_value > 0?
- [ ] Do all Group B users have current_value = 0?
- [ ] What are the specific violations and their counts?

### Tasks:
- [ ] Check Group A violations (revenue-only users with current_value = 0)
- [ ] Check Group B violations (refunded users with current_value > 0)
- [ ] Quantify the scope of violations

### Findings:
*To be filled as analysis progresses*

---

## Phase 4: Deep Dive on Violations
**Status**: ⏳ PENDING

### Questions to Answer:
- [ ] What's the exact event sequence for each violation?
- [ ] Are there timing issues (refund before revenue, etc.)?
- [ ] Are there data quality issues (missing revenue amounts, etc.)?
- [ ] Are there edge cases in the value estimation logic?

### Tasks:
- [ ] For each violation, examine the complete event timeline
- [ ] Look for patterns in the violations
- [ ] Check for data quality issues
- [ ] Analyze the value estimation logic for these specific cases

### Findings:
*To be filled as analysis progresses*

---

## Phase 5: Edge Case Analysis & Categorization
**Status**: ⏳ PENDING

### Questions to Answer:
- [ ] What are the most common types of violations?
- [ ] Are there systematic issues vs one-off edge cases?
- [ ] What specific scenarios need to be handled in the code?

### Tasks:
- [ ] Categorize violations by type/pattern
- [ ] Prioritize by frequency and impact
- [ ] Provide actionable recommendations

### Findings:
*To be filled as analysis progresses*

---

## Analysis Log

### Database Connection
- **Database Path**: `/Users/joshuakaufman/untitled folder 3/database/mixpanel_data.db`
- **Schema Reference**: `database/schema.sql`
- **Value Estimation Code**: `pipelines/pre_processing_pipeline/03_estimate_values.py`

### Key Tables:
- `user_product_metrics` - Contains current_value estimates
- `mixpanel_event` - Contains revenue/refund events
- `mixpanel_user` - User profile data

### Key Fields:
- `current_value` - The estimated value we're validating
- `event_name` - Type of event (RC Initial purchase, RC Trial converted, RC Cancellation)
- `revenue_usd` - Revenue amount (positive for purchases, negative for refunds)

---

## Detailed Findings

*This section will be populated as analysis progresses with specific findings, data points, and insights*

---

## Final Summary & Recommendations

*To be completed at the end of analysis*

### Critical Issues Found:
*To be filled*

### Recommended Actions:
*To be filled*

### Code Changes Needed:
*To be filled* 