# Ads Dashboard

A comprehensive React-based dashboard for analytics pipeline management, integrated with the orchestrator system.

## Directory Structure

```
dashboard/
├── client/                 # React application source code
│   ├── src/
│   │   ├── components/     # React components
│   │   ├── pages/         # Page components
│   │   ├── services/      # API service files
│   │   ├── hooks/         # Custom React hooks
│   │   ├── App.js         # Main application component
│   │   └── index.js       # React entry point
│   ├── public/            # Public assets
│   ├── package.json       # Dependencies and scripts
│   └── build/             # Built application (auto-generated)
├── static/                # Served static files (copied from build/)
├── api/                   # Flask API routes
└── services/              # Backend services

```

## Development Setup

### Prerequisites
- Node.js (v14 or higher)
- npm
- Python 3.8+

### Installing Dependencies

```bash
cd client/
npm install
```

### Development Mode

To run the React app in development mode:

```bash
cd client/
npm start
```

This will:
- Start the development server on `http://localhost:3000`
- Enable hot reloading for instant updates
- Show lint errors in the console

### Building for Production

To create a production build:

```bash
cd client/
npm run build
```

This creates a `build/` directory with optimized files.

### Deploying to Orchestrator

After building, copy the files to the static directory:

```bash
# From the dashboard/ directory
cp -r client/build/* static/
```

The orchestrator Flask app will serve these files via:
- `/ads-dashboard` - Main dashboard route
- `/dashboard-static/` - Static assets (CSS, JS, images)

## API Integration

The dashboard connects to the orchestrator's API endpoints:

### Dashboard API (`/api/dashboard/`)
- `GET /api/dashboard/configurations` - Get available data configurations
- `POST /api/dashboard/data` - Get dashboard data

### Analytics API (`/api/analytics-pipeline/`)
- `GET /api/analytics-pipeline/status` - Pipeline status
- `POST /api/analytics-pipeline/cancel` - Cancel operations

### Other APIs
- Meta API (`/api/meta/`)
- Mixpanel API (`/api/mixpanel/`)
- Cohort Analysis (`/api/cohort-analysis`)
- Conversion Probability (`/api/conversion-probability/`)
- Pricing Management (`/api/pricing/`)

## Features

### Main Dashboard
- Campaign performance analytics
- Real-time data visualization
- Interactive charts and graphs

### Cohort Analysis
- User cohort tracking
- Retention analysis
- Revenue attribution

### Conversion Probability
- User conversion predictions
- Property-based analysis
- Hierarchical analysis tools

### Pricing Management
- Dynamic pricing rules
- Product configuration
- Historical tracking

### Debug Tools
- Mixpanel data debugging
- Meta API testing
- Pipeline monitoring

## Tech Stack

### Frontend
- **React 18** - UI framework
- **React Router** - Navigation
- **Chart.js** - Data visualization
- **Tailwind CSS** - Styling
- **Axios** - HTTP client
- **Lucide React** - Icons

### Backend Integration
- **Flask** - Python web framework
- **SQLite** - Database
- **WebSocket** - Real-time updates

## Development Guidelines

### File Organization
- Components in `src/components/`
- Pages in `src/pages/`
- API calls in `src/services/`
- Shared hooks in `src/hooks/`

### Styling
- Use Tailwind CSS classes
- Custom styles in component-specific CSS files
- Dark mode support via `dark:` prefixes

### API Calls
- All API calls go through service files
- Use consistent error handling
- Include loading states

### State Management
- Use React hooks for local state
- Custom hooks for shared logic
- Props for component communication

## Available Scripts

In the `client/` directory:

- `npm start` - Development server
- `npm run build` - Production build
- `npm test` - Run tests
- `npm run eject` - Eject from Create React App (⚠️ irreversible)

## Troubleshooting

### Common Issues

1. **API Connection Failed**
   - Ensure orchestrator is running on port 5001
   - Check Flask API endpoints are active

2. **Build Errors**
   - Run `npm install` to ensure dependencies
   - Check for syntax errors in console

3. **Static Files Not Loading**
   - Rebuild with `npm run build`
   - Copy files to `static/` directory
   - Restart orchestrator

### Debug Mode
- Open browser dev tools
- Check console for errors
- Monitor network tab for failed requests

## Deployment

The dashboard is served by the orchestrator Flask application:

1. Build the React app: `npm run build`
2. Copy files: `cp -r client/build/* static/`
3. Start orchestrator: `python app.py`
4. Access at: `http://localhost:5001/ads-dashboard`

## Contributing

1. Make changes in `client/src/`
2. Test in development mode
3. Build for production
4. Copy to static directory
5. Test with orchestrator

## Support

For issues or questions, check:
- Browser console for errors
- Orchestrator logs for API issues
- Network tab for failed requests 