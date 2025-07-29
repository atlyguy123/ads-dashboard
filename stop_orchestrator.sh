#!/bin/bash

# Stop script for both Docker and Development modes
# Ensures clean shutdown of all services

echo "🛑 Stopping Ads Dashboard Services..."

# Check for running development servers
DEV_FLASK_RUNNING=false
DEV_REACT_RUNNING=false
DOCKER_RUNNING=false

if lsof -Pi :5001 -sTCP:LISTEN -t >/dev/null 2>&1; then
    if pgrep -f "python.*launcher.py" >/dev/null 2>&1; then
        DEV_FLASK_RUNNING=true
    fi
fi

if lsof -Pi :3000 -sTCP:LISTEN -t >/dev/null 2>&1; then
    if pgrep -f "react-scripts start" >/dev/null 2>&1; then
        DEV_REACT_RUNNING=true
    fi
fi

if command -v docker &> /dev/null && docker ps --format "table {{.Names}}" | grep -q "ads-dashboard-final"; then
    DOCKER_RUNNING=true
fi

if [ "$DEV_FLASK_RUNNING" = true ] || [ "$DEV_REACT_RUNNING" = true ]; then
    echo "🔧 Stopping development servers..."
    
    if [ "$DEV_FLASK_RUNNING" = true ]; then
        echo "🐍 Stopping Flask backend..."
        pkill -f "python.*launcher.py" 2>/dev/null || true
    fi
    
    if [ "$DEV_REACT_RUNNING" = true ]; then
        echo "⚛️  Stopping React dev server..."
        pkill -f "react-scripts start" 2>/dev/null || true
    fi
    
    echo ""
    echo "✅ Development servers stopped successfully!"
    echo ""
    echo "📊 Status:"
    echo "  🐍 Flask backend: STOPPED"
    echo "  ⚛️  React dev server: STOPPED"
    echo "  🌐 Port 5001: AVAILABLE"
    echo "  🌐 Port 3000: AVAILABLE"
    echo "  💾 Database: PERSISTED (in ./database)"
    
elif [ "$DOCKER_RUNNING" = true ]; then
    echo "🐳 Stopping Docker containers..."

    # Stop all containers and clean up
    docker-compose down

    # Also stop any orphaned containers
    echo "🔄 Cleaning up any orphaned containers..."
    docker ps -q --filter "ancestor=ads-dashboard-final_web" | xargs -r docker stop
    docker ps -q --filter "ancestor=ads-dashboard-final-web" | xargs -r docker stop

    echo ""
    echo "✅ Docker services stopped successfully!"
    echo ""
    echo "📊 Status:"
    echo "  🐳 Docker containers: STOPPED"
    echo "  🌐 Port 5001: AVAILABLE"
    echo "  💾 Database: PERSISTED (in ./database)"
    
else
    echo "ℹ️  No dashboard services appear to be running"
    echo ""
    echo "📊 Status:"
    echo "  🌐 Port 5001: AVAILABLE"
    echo "  🌐 Port 3000: AVAILABLE"
fi

# Final cleanup of any lingering processes
echo "🧹 Final cleanup..."
pkill -9 -f "python.*launcher.py" 2>/dev/null || true
pkill -9 -f "react-scripts start" 2>/dev/null || true
pkill -9 -f "launcher.py" 2>/dev/null || true

# Clean up startup scripts
if [ -f "start_flask_dev.sh" ]; then
    rm start_flask_dev.sh
fi
if [ -f "start_react_dev.sh" ]; then
    rm start_react_dev.sh
fi

echo "📺 Note: Separate terminal windows may still be open"
echo "💡 You can manually close the Flask and React terminal windows"

echo ""
echo "🚀 To restart:"
echo "  📦 Production mode: ./start_orchestrator.sh"
echo "  💻 Development mode: ./start_orchestrator.sh --dev" 