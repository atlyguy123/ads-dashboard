# Pipeline Debug System Documentation

## 🔧 Overview

The Pipeline Debug System provides a modern, React-based interface for debugging different stages of the data processing pipeline. It's designed to be self-contained, extensible, and integrated seamlessly with the existing dashboard.

## 🏗️ Architecture

### System Design
- **Frontend**: React pages integrated into the main dashboard
- **Backend**: Flask API routes with modular handlers
- **Integration**: Fully integrated with existing React router and UI patterns
- **Isolation**: Self-contained code that doesn't interfere with main application

### Key Components
1. **React Debug Pages** - Modern UI components for each debug tool
2. **API Services** - Centralized API communication layer
3. **Backend Handlers** - Modular Python handlers for debug logic
4. **Routing Integration** - Seamless navigation within the main dashboard

## 📁 File Structure

```
orchestrator/
├── debug/                                    # Backend debug system
│   ├── README.md                            # This documentation
│   ├── __init__.py                          # Debug system initialization
│   ├── registry.py                          # Module registry (legacy)
│   │
│   ├── api/                                 # API endpoints
│   │   ├── __init__.py
│   │   └── debug_routes.py                  # Clean API routes for debug tools
│   │
│   ├── modules/                             # Debug module implementations
│   │   ├── __init__.py                      # Module definitions
│   │   ├── conversion_rates_debug/          # Conversion rates debug module
│   │   │   ├── __init__.py
│   │   │   ├── config.yaml                  # Module configuration
│   │   │   ├── handlers.py                  # Backend logic (ready for implementation)
│   │   │   └── interface.html               # Legacy HTML (not used in React)
│   │   ├── price_bucket_debug/              # Price bucket debug module
│   │   │   ├── config.yaml
│   │   │   ├── handlers.py                  # Backend logic (ready for implementation)
│   │   │   └── interface.html               # Legacy HTML (not used in React)
│   │   └── value_estimation_debug/          # Value estimation debug module
│   │       ├── __init__.py
│   │       ├── config.yaml
│   │       ├── handlers.py                  # Backend logic (ready for implementation)
│   │       └── interface.html               # Legacy HTML (not used in React)
│   │
│   └── templates/
│       └── debug.html                       # Legacy template with redirect info
│
└── dashboard/client/src/                    # Frontend React code
    ├── App.js                               # Updated with debug routes
    ├── pages/debug/                         # Debug page components
    │   ├── PipelineDebugPage.js            # Main debug hub/landing
    │   ├── ConversionRatesDebugPage.js     # Conversion rates debug UI
    │   ├── PriceBucketDebugPage.js         # Price bucket debug UI
    │   └── ValueEstimationDebugPage.js     # Value estimation debug UI
    │
    └── services/
        └── debugApi.js                      # API service layer
```

## 🚀 How to Access Debug Tools

### Via Dashboard Navigation
1. Navigate to the main dashboard
2. Look for the **"🔧 Pipeline Debug"** link in the top navigation bar
3. Click to access the main debug hub
4. Select any debug tool from the hub interface

### Direct URLs
- **Main Hub**: `/pipeline-debug`
- **Conversion Rates**: `/debug/conversion-rates`
- **Price Bucket**: `/debug/price-bucket`
- **Value Estimation**: `/debug/value-estimation`

## 🛠️ How to Add New Debug Tools

### Step 1: Create Backend Module
1. Create a new directory in `orchestrator/debug/modules/`:
   ```
   orchestrator/debug/modules/my_new_debug/
   ├── __init__.py
   ├── config.yaml
   ├── handlers.py
   └── interface.html  (optional, for legacy compatibility)
   ```

2. Configure the module in `config.yaml`:
   ```yaml
   name: "my_new_debug"
   display_name: "My New Debug Tool"
   description: "Debug tool for my specific pipeline stage"
   version: "1.0.0"
   enabled: true
   ```

3. Implement handlers in `handlers.py`:
   ```python
   # My New Debug Handlers
   # This module contains handlers for debugging my pipeline stage
   
   def placeholder():
       """Placeholder function - implement your debug logic here"""
       pass
   ```

### Step 2: Add API Routes
1. Open `orchestrator/debug/api/debug_routes.py`
2. Add new routes for your debug tool:
   ```python
   @debug_bp.route('/api/debug/my-new-debug/overview', methods=['POST'])
   def my_new_debug_overview():
       """Get my new debug overview data"""
       try:
           # Call your handler functions here
           return jsonify({
               'success': True,
               'data': {
                   'message': 'My new debug data',
                   'placeholder': True
               }
           })
       except Exception as e:
           logger.error(f"Error in my new debug overview: {e}")
           return jsonify({'success': False, 'error': str(e)})
   ```

### Step 3: Create React Components
1. Create a new React page in `orchestrator/dashboard/client/src/pages/debug/`:
   ```javascript
   // MyNewDebugPage.js
   import React, { useState } from 'react';
   
   const MyNewDebugPage = () => {
     const [loading, setLoading] = useState(false);
     const [data, setData] = useState(null);
     const [error, setError] = useState(null);
   
     const handleLoadData = async () => {
       setLoading(true);
       setError(null);
       
       try {
         const response = await fetch('/api/debug/my-new-debug/overview', {
           method: 'POST',
           headers: { 'Content-Type': 'application/json' },
           body: JSON.stringify({})
         });
         const result = await response.json();
         setData(result);
       } catch (err) {
         setError('Failed to load debug data');
       } finally {
         setLoading(false);
       }
     };
   
     return (
       <div className="p-6 max-w-7xl mx-auto">
         {/* Your debug UI here */}
       </div>
     );
   };
   
   export default MyNewDebugPage;
   ```

### Step 4: Add to API Services
1. Update `orchestrator/dashboard/client/src/services/debugApi.js`:
   ```javascript
   // Add to existing API services
   export const myNewDebugApi = {
     loadOverview: () => apiCall('/my-new-debug/overview'),
     // Add more API calls as needed
   };
   ```

### Step 5: Add Navigation and Routing
1. Update `orchestrator/dashboard/client/src/App.js`:
   ```javascript
   // Add import
   import MyNewDebugPage from './pages/debug/MyNewDebugPage';
   
   // Add route
   <Route path="/debug/my-new-debug" element={<MyNewDebugPage />} />
   ```

2. Update `orchestrator/dashboard/client/src/pages/debug/PipelineDebugPage.js`:
   ```javascript
   // Add to debugTools array
   {
     id: 'my-new-debug',
     title: 'My New Debug Tool',
     description: 'Debug tool for my specific pipeline stage',
     icon: '🆕',
     path: '/debug/my-new-debug',
     status: 'ready'
   }
   ```

## 🔌 API System

### Request/Response Pattern
All debug API endpoints follow this pattern:
- **Method**: POST
- **Content-Type**: application/json
- **Request Body**: JSON with parameters
- **Response**: Standardized JSON response

```javascript
// Request
{
  "parameter1": "value1",
  "parameter2": "value2"
}

// Response
{
  "success": true,
  "data": {
    // Your debug data here
  }
}

// Error Response
{
  "success": false,
  "error": "Error message"
}
```

### Available API Endpoints
- `POST /api/debug/conversion-rates/overview` - Conversion rates overview
- `POST /api/debug/conversion-rates/cohort-tree` - Cohort tree data
- `POST /api/debug/price-bucket/overview` - Price bucket overview
- `POST /api/debug/price-bucket/assignments` - Assignment data
- `POST /api/debug/value-estimation/timeline` - Timeline data
- `POST /api/debug/value-estimation/refresh` - Refresh data
- `POST /api/debug/modules/<module>/actions/<action>` - Generic module actions

## 🎨 UI/UX Guidelines

### Design Patterns
- Follow existing dashboard design language
- Use Tailwind CSS classes for styling
- Implement dark mode support
- Include loading states and error handling
- Use consistent card-based layouts

### Component Structure
```javascript
const DebugPage = () => {
  return (
    <div className="p-6 max-w-7xl mx-auto">
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow-md p-6">
        {/* Header */}
        <div className="mb-6">
          <h1 className="text-3xl font-bold text-gray-900 dark:text-white mb-2">
            Debug Tool Title
          </h1>
          <p className="text-gray-600 dark:text-gray-300">
            Description of what this tool does
          </p>
        </div>

        {/* Controls */}
        <div className="bg-gray-50 dark:bg-gray-700 rounded-lg p-4">
          {/* Control buttons and inputs */}
        </div>

        {/* Results */}
        <div className="space-y-6">
          {/* Error handling */}
          {/* Data display */}
          {/* Placeholder content */}
        </div>
      </div>
    </div>
  );
};
```

## 🧪 Testing Your Debug Tools

### Frontend Testing
1. Start the React development server
2. Navigate to your debug tool URL
3. Test all interactive elements
4. Verify error handling works
5. Check responsive design

### Backend Testing
1. Use curl or Postman to test API endpoints:
   ```bash
   curl -X POST http://localhost:5001/api/debug/my-new-debug/overview \
        -H "Content-Type: application/json" \
        -d '{}'
   ```

### Integration Testing
1. Test the full flow from UI button click to data display
2. Verify error states are handled gracefully
3. Test with different data scenarios

## 📋 Best Practices

### Code Organization
- Keep debug code self-contained and isolated
- Use consistent naming conventions
- Follow existing patterns for new tools
- Document your debug handlers clearly

### Error Handling
- Always wrap API calls in try/catch blocks
- Provide meaningful error messages to users
- Log errors server-side for debugging
- Implement loading states for better UX

### Performance
- Implement data pagination for large datasets
- Add loading indicators for long operations
- Consider caching frequently accessed data
- Optimize database queries in handlers

### Security
- Validate all input parameters
- Don't expose sensitive data in debug outputs
- Use appropriate logging levels
- Follow existing authentication patterns

## 🚨 Troubleshooting

### Common Issues
1. **404 on debug routes**: Check that routes are properly added to App.js
2. **API errors**: Verify backend routes match frontend API calls
3. **Import errors**: Ensure all React components are properly exported
4. **Styling issues**: Check Tailwind classes and dark mode support

### Debug System Health
- Check if debug blueprint is registered in main app
- Verify module configurations are valid YAML
- Ensure all required dependencies are installed
- Test API endpoints individually before UI integration

## 🔄 Maintenance

### Updating Debug Tools
1. Update handler functions for new debug logic
2. Add new API endpoints as needed
3. Extend React components with new features
4. Update this documentation when adding major features

### Monitoring
- Monitor API endpoint performance
- Track usage of different debug tools
- Keep debug tools updated with pipeline changes
- Regular cleanup of old debug data if applicable

---

**Need Help?** Check the existing debug tools as examples, or refer to the main dashboard patterns for UI consistency. 