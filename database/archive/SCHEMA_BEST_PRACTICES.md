# Database Schema Management Best Practices

## 🎯 **SINGLE SOURCE OF TRUTH ESTABLISHED**

You now have **ONE authoritative schema**: `database/schema.sql`

---

## ✅ **Current Clean State**

### **Active Files:**
- ✅ `database/schema.sql` - **AUTHORITATIVE SCHEMA** (single source of truth)
- ✅ `database/mixpanel_data.db` - **LIVE DATABASE** (consolidated)

### **Archived Files:**
- 📁 `database/archive/mixpanel_analytics.db.backup` - Old analytics DB (archived)
- 📁 `database/archive/schema_merged.sql` - Temporary merge schema (archived)
- 📁 `database/archive/merge_databases.sql` - Merge script (archived)
- 📁 `database/schema_original_backup.sql` - Original schema backup

---

## 📋 **Best Practices Moving Forward**

### **1. Schema-First Development**
```bash
# ✅ DO: Always reference the authoritative schema
# All code should implement based on database/schema.sql

# ❌ DON'T: Create tables or modify structure without updating schema first
```

### **2. Schema Change Process**
When you need to modify the database structure:

1. **Update schema.sql FIRST**
2. **Create migration script**
3. **Apply to database**
4. **Update application code**
5. **Test thoroughly**

### **3. Code Alignment Checklist**

#### **Connection Strings:**
- [ ] Update all references from `mixpanel_analytics.db` to `mixpanel_data.db`
- [ ] Remove any dual-database connection logic

#### **Table References:**
- [ ] Change `fact_user_products` → `user_product_metrics` (consolidated table)
- [ ] Update `user_id` → `distinct_id` in queries
- [ ] Verify all column names match schema.sql

#### **Query Updates:**
```sql
-- ❌ OLD (analytics DB)
SELECT user_id, current_value FROM user_product_metrics;

-- ✅ NEW (consolidated)
SELECT distinct_id, current_value FROM user_product_metrics;
```

### **4. Schema Validation Commands**

#### **Verify Database Matches Schema:**
```bash
# Compare actual DB structure with schema
sqlite3 database/mixpanel_data.db ".schema" > actual_schema.txt
diff database/schema.sql actual_schema.txt
```

#### **Table Verification:**
```sql
-- Verify consolidated table exists and has correct structure
PRAGMA table_info(user_product_metrics);

-- Verify data migration was successful
SELECT COUNT(*) FROM user_product_metrics; -- Should be 13,799

-- Verify field mapping worked
SELECT distinct_id, product_id, current_value, valid_lifecycle 
FROM user_product_metrics LIMIT 5;
```

---

## 🔄 **Next Steps for Code Alignment**

### **Phase 1: Immediate Updates**
1. **Search & Replace** all references to:
   - `mixpanel_analytics.db` → `mixpanel_data.db`
   - `fact_user_products` → `user_product_metrics`
   - `user_id` → `distinct_id` (in user-product contexts)

### **Phase 2: Enhanced Capabilities**
2. **Leverage new consolidated data**:
   - Cross-join user analytics with attribution
   - Enhanced reporting with conversion metrics
   - Single-query user journey analysis

### **Phase 3: Cleanup**
3. **Remove legacy code**:
   - Analytics DB connection logic
   - Dual-database synchronization code
   - Separate user-product table handling

---

## 🏗️ **Schema Maintenance Rules**

### **✅ DO:**
- Keep `schema.sql` as the single source of truth
- Update schema before making DB changes
- Document all schema changes with comments
- Version control all schema modifications
- Test schema changes on dev environment first

### **❌ DON'T:**
- Modify database structure without updating schema.sql
- Create temporary tables without documenting them
- Make "quick fixes" directly to the database
- Maintain multiple schema files for the same database

---

## 🎯 **Success Metrics**

Your schema management is successful when:
- [ ] All code references `database/schema.sql` as authority
- [ ] Database structure exactly matches schema.sql
- [ ] No references to archived databases remain in code
- [ ] All analytics capabilities work from single database
- [ ] Team members know to check schema.sql first

---

## 📞 **When to Update This Guide**

Update this guide when you:
- Add new tables or significantly modify existing ones
- Change the schema management process
- Add new development team members
- Implement database migration tools

**Remember: One schema, one database, one source of truth! 🎯** 