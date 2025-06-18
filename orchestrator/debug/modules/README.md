# Debug Modules

This directory contains debug modules for different system components. Each debug module provides custom debug interfaces and tools.

## Creating a Debug Module

To create a new debug module, follow these steps:

### 1. Create Module Directory
Create a new directory with the format `{system_name}_debug/`:
```
orchestrator/debug/modules/my_module_debug/
```

### 2. Required Files

Each debug module **must** contain these files:

#### `config.yaml` - Module Configuration
```yaml
name: "my_module_debug"
display_name: "My Module Debug"
description: "Debug tools for My Module system"
version: "1.0.0"
enabled: true
author: "Your Name"
```

#### `interface.html` - Frontend Interface
```html
<h3>My Module Debug Tools</h3>
<p>Custom debug interface for My Module</p>

<div class="form-group">
    <button class="btn btn-primary" data-action="test_action">Test Action</button>
</div>

<div class="result-container">
    <p>Results will appear here...</p>
</div>
```

#### `handlers.py` - Backend Handlers
```python
"""
Backend handlers for My Module debug actions
"""

def handle_test_action(request_data):
    """Handle the test_action from the frontend"""
    return {
        'success': True,
        'data': 'Test action completed successfully!',
        'timestamp': '2025-01-23T12:00:00'
    }
```

### 3. Frontend Interface Guidelines

- Use `data-action` attributes on buttons to define actions
- Use `data-action` attributes on forms for form submissions
- Include `.result-container` divs to display results
- Follow the existing CSS classes for consistent styling

### 4. Backend Handler Guidelines

- Name handler functions as `handle_{action_name}`
- Always return a dictionary with at least `success` field
- Include error handling and meaningful error messages
- Return structured data that can be displayed in the frontend

### 5. Module Discovery

The debug system automatically discovers modules by:
1. Scanning this directory for subdirectories
2. Looking for `config.yaml` in each subdirectory  
3. Validating that required files exist
4. Loading modules that pass validation

### 6. Example Module Structure

```
my_module_debug/
├── config.yaml           # Module configuration
├── interface.html         # Frontend interface
├── handlers.py           # Backend action handlers
└── __init__.py           # Optional Python package file
```

### 7. Testing Your Module

1. Create your module directory and files
2. Restart the orchestrator or refresh modules via `/api/debug/refresh`
3. Navigate to `/debug` in your browser
4. Your module should appear in the sidebar
5. Click on it to load the interface and test actions

### 8. Common Actions

Some common debug actions you might implement:
- `refresh_data` - Refresh data from database
- `test_connection` - Test API or database connections  
- `clear_cache` - Clear cached data
- `validate_config` - Validate configuration
- `run_diagnostics` - Run system diagnostics
- `export_data` - Export data for analysis

### 9. Error Handling

Always handle errors gracefully in your handlers:

```python
def handle_risky_action(request_data):
    try:
        # Your code here
        result = do_something_risky()
        return {'success': True, 'data': result}
    except Exception as e:
        return {
            'success': False, 
            'error': str(e),
            'message': 'Action failed due to an error'
        }
``` 