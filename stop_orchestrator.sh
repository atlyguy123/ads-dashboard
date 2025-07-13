#!/bin/bash

echo "🛑 Stopping Ads Dashboard Services..."

# Stop Flask backend
echo "🔧 Stopping Backend (Flask)..."
pkill -f "python.*app.py" && echo "   ✅ Backend stopped" || echo "   ℹ️  No backend processes found"

# Stop React frontend
echo "🌐 Stopping Frontend (React)..."
pkill -f "npm.*start" && echo "   ✅ Frontend stopped" || echo "   ℹ️  No frontend processes found"

# Also stop any react-scripts processes
pkill -f "react-scripts start" && echo "   ✅ React scripts stopped" || echo "   ℹ️  No react-scripts processes found"

echo ""
echo "🎉 All services stopped!"
echo "You can restart them with: ./start_orchestrator.sh" 