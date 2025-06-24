import { 
  AVAILABLE_COLUMNS, 
  getColumnByKey, 
  getDefaultVisibleColumns, 
  getAllColumnKeys,
  validateColumnOrder,
  migrateColumnOrder,
  migrateColumnVisibility
} from './columns';

describe('Dashboard Column Configuration', () => {
  
  test('AVAILABLE_COLUMNS should be defined and valid', () => {
    expect(AVAILABLE_COLUMNS).toBeDefined();
    expect(Array.isArray(AVAILABLE_COLUMNS)).toBe(true);
    expect(AVAILABLE_COLUMNS.length).toBeGreaterThan(0);
  });

  test('All columns should have required properties', () => {
    AVAILABLE_COLUMNS.forEach(col => {
      expect(col).toHaveProperty('key');
      expect(col).toHaveProperty('label');
      expect(col).toHaveProperty('defaultVisible');
      expect(typeof col.key).toBe('string');
      expect(typeof col.label).toBe('string');
      expect(typeof col.defaultVisible).toBe('boolean');
    });
  });

  test('Column keys should be unique', () => {
    const keys = AVAILABLE_COLUMNS.map(col => col.key);
    const uniqueKeys = [...new Set(keys)];
    expect(keys.length).toBe(uniqueKeys.length);
  });

  test('Required columns should exist', () => {
    const requiredColumns = [
      'name',
      'estimated_revenue_adjusted', 
      'mixpanel_revenue_net',
      'estimated_roas',
      'profit'
    ];
    
    requiredColumns.forEach(key => {
      expect(getColumnByKey(key)).toBeDefined();
    });
  });

  test('migrateColumnOrder should handle missing columns', () => {
    const savedOrder = ['name', 'spend']; // Missing new columns
    const migrated = migrateColumnOrder(savedOrder);
    
    expect(migrated).toContain('estimated_revenue_adjusted');
    expect(migrated).toContain('mixpanel_revenue_net');
    expect(migrated.length).toBe(AVAILABLE_COLUMNS.length);
  });

  test('migrateColumnVisibility should RESPECT user preferences (never override)', () => {
    const savedVisibility = { 'estimated_revenue_adjusted': false }; // User explicitly hid it
    const migrated = migrateColumnVisibility(savedVisibility);
    
    // Should RESPECT user's choice, NOT force to true
    expect(migrated.estimated_revenue_adjusted).toBe(false); // User hid it, keep it hidden
    expect(migrated.mixpanel_revenue_net).toBe(true); // New column, use default
  });

  test('validateColumnOrder should catch missing columns', () => {
    const incompleteOrder = ['name', 'spend']; // Missing columns
    const validation = validateColumnOrder(incompleteOrder);
    
    expect(validation.isValid).toBe(false);
    expect(validation.missing.length).toBeGreaterThan(0);
    expect(validation.missing).toContain('estimated_revenue_adjusted');
  });

  test('Helper functions should work correctly', () => {
    expect(getDefaultVisibleColumns().length).toBeGreaterThan(0);
    expect(getAllColumnKeys().length).toBe(AVAILABLE_COLUMNS.length);
    
    const nameColumn = getColumnByKey('name');
    expect(nameColumn).toBeDefined();
    expect(nameColumn.key).toBe('name');
  });

}); 