#!/bin/bash

# Master script to start all components in separate Terminal tabs
# This script starts:
# - Backend (Flask) on port 5001
# - Frontend (React) on port 3000

echo "🚀 Starting Ads Dashboard Pipeline System..."
echo "This will automatically open separate Terminal tabs for each service"
echo "================================================="

# Get the absolute path of the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Function to check if a port is in use
check_port() {
    local port=$1
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        echo "⚠️  Port $port is already in use"
        return 1
    fi
    return 0
}

# Check if ports are available
echo "🔍 Checking port availability..."
check_port 5001 || { echo "Please stop the service on port 5001 first"; exit 1; }
check_port 3000 || { echo "Please stop the service on port 3000 first"; exit 1; }

echo "✅ All ports are available"

# Check if virtual environment exists
if [ ! -d "$SCRIPT_DIR/venv" ]; then
    echo "❌ Virtual environment not found at venv/"
    echo "🔄 Creating virtual environment..."
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
else
    echo "✅ Virtual environment found"
fi

# Check if frontend node_modules exists
if [ ! -d "$SCRIPT_DIR/orchestrator/dashboard/client/node_modules" ]; then
    echo "📦 Installing frontend dependencies..."
    cd "$SCRIPT_DIR/orchestrator/dashboard/client"
    npm install
    cd "$SCRIPT_DIR"
else
    echo "✅ Frontend dependencies found"
fi

echo ""

# Start Backend in new terminal tab
echo "🔧 Opening terminal tab for Backend (port 5001)..."
osascript -e "
tell application \"Terminal\"
    activate
    tell application \"System Events\" to tell process \"Terminal\" to keystroke \"t\" using command down
    do script \"cd '$SCRIPT_DIR' && echo '🔧 Starting Backend (Flask)...' && echo 'Port: 5001' && source venv/bin/activate && cd orchestrator && python3 app.py\" in front window
end tell
"

# Wait a moment before starting frontend
sleep 2

# Start Frontend in new terminal tab
echo "🌐 Opening terminal tab for Frontend (port 3000)..."
osascript -e "
tell application \"Terminal\"
    activate
    tell application \"System Events\" to tell process \"Terminal\" to keystroke \"t\" using command down
    do script \"cd '$SCRIPT_DIR/orchestrator/dashboard/client' && echo '🌐 Starting Frontend (React)...' && echo 'Port: 3000' && npm start\" in front window
end tell
"

echo ""
echo "🎉 All services are starting in separate terminal tabs!"
echo "================================================="
echo "📊 Service URLs:"
echo "   🔧 Backend API:   http://localhost:5001"
echo "   🌐 Frontend:      http://localhost:3000"
echo ""
echo "📋 Each service is running in its own terminal tab where you can:"
echo "   - See live output and logs"
echo "   - Stop individual services with Ctrl+C"
echo "   - Switch between tabs with Cmd+1, Cmd+2, Cmd+3"
echo ""
echo "🛑 To stop all services:"
echo "   - Use Ctrl+C in each terminal tab"
echo ""
echo "✨ Happy coding!" 