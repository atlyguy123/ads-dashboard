# Dashboard Column Management

## 🚨 CRITICAL: How to Add New Columns

**ALWAYS follow this checklist when adding new dashboard columns:**

### ✅ Step-by-Step Process

1. **Add to Column Config** 
   ```javascript
   // Edit: src/config/columns.js
   { 
     key: 'new_column_key', 
     label: 'Display Name', 
     defaultVisible: true|false 
   }
   ```

2. **Update Backend API**
   - Ensure your new field exists in the analytics API response
   - Test that the field has the expected data type

3. **Add Rendering Logic**
   ```javascript
   // Edit: src/components/DashboardGrid.js > renderCellValue()
   case 'new_column_key':
     formattedValue = formatCurrency(value); // or appropriate formatter
     break;
   ```

4. **Test & Validate**
   - Refresh dashboard and check console for validation errors
   - Toggle column visibility to ensure it works
   - Verify data displays correctly

### 🛡️ Built-in Safety Mechanisms

- **Auto-Migration**: New columns automatically appear in user's column order
- **Runtime Validation**: Console warnings if column config is inconsistent  
- **Single Source of Truth**: All files import from `/config/columns.js`

### 🚫 Common Mistakes to Avoid

❌ **DON'T** define columns in multiple files  
❌ **DON'T** modify localStorage directly  
❌ **DON'T** forget to add rendering logic  
❌ **DON'T** skip testing column visibility toggles  

### 🔍 Debugging Column Issues

1. Check browser console for validation warnings
2. Look for `🚨 Column Order Validation Failed` or `🚨 Column Visibility Validation Failed`
3. Verify the field exists in API response
4. Ensure `renderCellValue()` handles your column key

### 📝 Example: Adding "Conversion Rate" Column

```javascript
// 1. Add to columns.js
{ 
  key: 'conversion_rate', 
  label: 'Conversion Rate', 
  defaultVisible: true 
}

// 2. Add rendering in DashboardGrid.js
case 'conversion_rate':
  formattedValue = value !== undefined ? `${(value * 100).toFixed(2)}%` : 'N/A';
  break;
```

**Result**: Column automatically appears for all users, with proper formatting and validation.

### 🧪 Testing Column Changes

After making any column changes, test these scenarios:

1. **Column Visibility Test**:
   - Hide all columns → Check console for `💾 Column visibility saved to localStorage`
   - Show only new columns → Verify they appear correctly
   - Refresh page → Columns should maintain their visibility state

2. **Column Reordering Test**:
   - Drag columns to reorder → Check console for `💾 Column order saved to localStorage`
   - Refresh page → Column order should be preserved
   - Add new column → Should appear at end of existing order

3. **LocalStorage Persistence Test**:
   ```javascript
   // In browser console:
   console.log('Column Order:', JSON.parse(localStorage.getItem('dashboard_column_order')));
   console.log('Column Visibility:', JSON.parse(localStorage.getItem('dashboard_column_visibility')));
   ```

4. **Validation Test**:
   - Check console for `✅ Column validation passed` (no warnings)
   - Look for any `🚨 Column Order Validation Failed` errors

### 🔧 Troubleshooting Checklist

If columns aren't working:
- [ ] Column added to `src/config/columns.js`
- [ ] Field exists in backend API response
- [ ] Rendering case added to `DashboardGrid.js` 
- [ ] No validation errors in console
- [ ] localStorage contains new column
- [ ] Browser hard refresh (Cmd+Shift+R) 