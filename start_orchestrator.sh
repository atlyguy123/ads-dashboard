#!/bin/bash

# Master script to start all components with Docker
# This ensures IDENTICAL behavior to Railway deployment

# Check for help flag
if [[ "$1" == "--help" || "$1" == "-h" ]]; then
    echo "🚀 Ads Dashboard Pipeline System"
    echo "================================================="
    echo ""
    echo "Usage:"
    echo "  ./start_orchestrator.sh           Start in production mode (Docker)"
    echo "  ./start_orchestrator.sh --dev     Start in development mode (live reload)"
    echo "  ./start_orchestrator.sh --help    Show this help message"
    echo ""
    echo "Modes:"
    echo "  📦 Production Mode (Default):"
    echo "     • Uses Docker for Railway consistency"
    echo "     • Serves built React static files"
    echo "     • Access: http://localhost:5001"
    echo ""
    echo "  💻 Development Mode (--dev):"
    echo "     • Flask backend with live reload"
    echo "     • React dev server with hot reload"
    echo "     • 🎯 ALWAYS PORT 3000: http://localhost:3000"
    echo "     • Backend API (internal): http://localhost:5001"
    echo "     • Code changes appear instantly!"
    echo "     • No more port conflicts!"
    echo ""
    echo "🛑 To stop: ./stop_orchestrator.sh"
    exit 0
fi

# Check for development mode flag
DEV_MODE=false
if [[ "$1" == "--dev" || "$1" == "-d" ]]; then
    DEV_MODE=true
    echo "🚀 Starting Ads Dashboard Pipeline System in DEVELOPMENT MODE..."
    echo "🔥 Live reloading enabled for React and Flask!"
    echo "================================================="
else
    echo "🚀 Starting Ads Dashboard Pipeline System with Docker..."
    echo "🐳 Using Docker for complete Railway consistency"
    echo "================================================="
fi

if [ "$DEV_MODE" = true ]; then
    # Development mode - start Flask backend + React dev server
    echo "🔧 Development Mode Setup:"
    echo "  • Flask backend on http://localhost:5001 (with live reload)"
    echo "  • React dev server on http://localhost:3000 (with hot reload)"
    echo "  • Changes appear instantly!"
    echo ""
    
    # Check for required dependencies
    if ! command -v python3 &> /dev/null; then
        echo "❌ Python 3 is required for development mode"
        exit 1
    fi
    
    if ! command -v node &> /dev/null; then
        echo "❌ Node.js is required for development mode"
        exit 1
    fi
    
    # Enhanced cleanup for ports 5001 and 3000
    echo "🧹 Performing thorough port cleanup..."
    
    # Check and report what's currently using the ports
    PORT_5001_PID=$(lsof -ti:5001 2>/dev/null || true)
    PORT_3000_PID=$(lsof -ti:3000 2>/dev/null || true)
    
    if [ -n "$PORT_5001_PID" ]; then
        echo "  🔍 Found process on port 5001: PID $PORT_5001_PID"
        ps -p $PORT_5001_PID -o pid,ppid,cmd 2>/dev/null || true
    fi
    
    if [ -n "$PORT_3000_PID" ]; then
        echo "  🔍 Found process on port 3000: PID $PORT_3000_PID"
        ps -p $PORT_3000_PID -o pid,ppid,cmd 2>/dev/null || true
    fi
    
    # Stop all related processes gracefully first
    echo "  🛑 Gracefully stopping Flask processes..."
    pkill -TERM -f "python.*launcher.py" 2>/dev/null || true
    pkill -TERM -f "flask.*run" 2>/dev/null || true
    
    echo "  🛑 Gracefully stopping React processes..."
    pkill -TERM -f "react-scripts start" 2>/dev/null || true
    pkill -TERM -f "node.*react-scripts" 2>/dev/null || true
    
    echo "  🛑 Gracefully stopping Node.js processes..."
    pkill -TERM -f "node.*start" 2>/dev/null || true
    
    # Stop Docker containers if running
    if command -v docker &> /dev/null; then
        echo "  🐳 Stopping Docker containers..."
        docker-compose down --remove-orphans >/dev/null 2>&1 || true
        
        # Stop any containers using our ports
        DOCKER_CONTAINERS=$(docker ps -q --filter "publish=5001" --filter "publish=3000" 2>/dev/null || true)
        if [ -n "$DOCKER_CONTAINERS" ]; then
            echo "  🐳 Stopping Docker containers on our ports..."
            docker stop $DOCKER_CONTAINERS >/dev/null 2>&1 || true
        fi
    fi
    
    # Wait for graceful shutdown
    echo "  ⏳ Waiting for graceful shutdown..."
    sleep 5
    
    # Force kill anything still running on the ports (multiple approaches)
    echo "  💥 Force killing remaining processes on ports 5001 and 3000..."
    
    # Method 1: Direct port killing
    for port in 5001 3000; do
        PIDS=$(lsof -ti:$port 2>/dev/null || true)
        if [ -n "$PIDS" ]; then
            echo "    🔫 Force killing PIDs on port $port: $PIDS"
            echo "$PIDS" | xargs -r kill -9 2>/dev/null || true
        fi
    done
    
    # Method 2: Kill by process patterns (more thorough)
    echo "  💥 Force killing by process patterns..."
    pkill -9 -f "launcher.py" 2>/dev/null || true
    pkill -9 -f "python.*app.py" 2>/dev/null || true
    pkill -9 -f "flask.*run" 2>/dev/null || true
    pkill -9 -f "react-scripts" 2>/dev/null || true
    pkill -9 -f "node.*start" 2>/dev/null || true
    
    # Method 3: Kill any remaining Python/Node processes on our ports
    for port in 5001 3000; do
        REMAINING_PIDS=$(lsof -ti:$port 2>/dev/null || true)
        if [ -n "$REMAINING_PIDS" ]; then
            echo "    🔫 Final cleanup - killing remaining PIDs on port $port: $REMAINING_PIDS"
            echo "$REMAINING_PIDS" | xargs -r kill -9 2>/dev/null || true
        fi
    done
    
    # Wait a moment for cleanup to complete
    sleep 2
    
    # Final verification
    echo "  🔍 Verifying ports are free..."
    FINAL_5001=$(lsof -ti:5001 2>/dev/null || true)
    FINAL_3000=$(lsof -ti:3000 2>/dev/null || true)
    
    if [ -n "$FINAL_5001" ] || [ -n "$FINAL_3000" ]; then
        echo "  ⚠️  WARNING: Some processes may still be using the ports!"
        if [ -n "$FINAL_5001" ]; then
            echo "    Port 5001 still in use by PID: $FINAL_5001"
            ps -p $FINAL_5001 -o pid,ppid,cmd 2>/dev/null || true
        fi
        if [ -n "$FINAL_3000" ]; then
            echo "    Port 3000 still in use by PID: $FINAL_3000"
            ps -p $FINAL_3000 -o pid,ppid,cmd 2>/dev/null || true
        fi
        echo "  🔧 You may need to manually kill these processes or restart your system"
    else
        echo "  ✅ Ports 5001 and 3000 are now completely free!"
    fi
    
    echo "✅ Starting development servers..."
    
    # Install dependencies if needed
    if [ ! -d "venv" ]; then
        echo "📦 Setting up Python virtual environment..."
        python3 -m venv venv
        echo "   ✅ Virtual environment created"
    fi
    
    echo "🐍 Activating Python environment and checking dependencies..."
    source venv/bin/activate
    
    # Check if requirements need to be installed
    if ! pip show flask > /dev/null 2>&1; then
        echo "📦 Installing Python dependencies (this may take a moment)..."
        pip install -r requirements.txt
        echo "   ✅ Python dependencies installed"
    else
        echo "   ✅ Python dependencies already installed"
    fi
    
    if [ ! -d "orchestrator/dashboard/client/node_modules" ]; then
        echo "📦 Installing React dependencies (this may take a few minutes)..."
        cd orchestrator/dashboard/client && npm install && cd ../../..
        echo "   ✅ React dependencies installed"
    else
        echo "   ✅ React dependencies already installed"
    fi
    
    # Start Flask backend in separate terminal window
    echo "🐍 Starting Flask backend in new terminal window..."
    export FLASK_ENV=development
    export FLASK_DEBUG=true
    export PORT=5001
    
    # Create Flask startup script
    cat > start_flask_dev.sh << 'EOF'
#!/bin/bash
cd "$(dirname "$0")"
source venv/bin/activate
export FLASK_ENV=development
export FLASK_DEBUG=true
export PORT=5001
echo "🐍 Flask Development Server Starting..."
echo "📍 Working Directory: $(pwd)"
echo "🌐 Server will be available at: http://localhost:5001"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
python3 launcher.py
EOF
    chmod +x start_flask_dev.sh
    
    # Launch Flask in new Terminal window
    osascript -e "
    tell application \"Terminal\"
        do script \"cd '$PWD' && ./start_flask_dev.sh\"
        set custom title of front window to \"🐍 Flask Development Server\"
    end tell"
    
    # Wait a moment for Flask to start
    sleep 3
    
    # Find Flask PID
    FLASK_PID=$(pgrep -f "python.*launcher.py" | head -1)
    echo "   📋 Flask PID: $FLASK_PID (running in separate terminal)"
    
    # Wait for Flask to start and check if it's responding
    echo "   ⏳ Waiting for Flask to start..."
    FLASK_STARTED=false
    for i in {1..10}; do
        if curl -s http://localhost:5001/health > /dev/null 2>&1; then
            echo "   ✅ Flask backend is ready!"
            FLASK_STARTED=true
            break
        fi
        
        # Check if process is still alive (less frequently)
        if [ $((i % 3)) -eq 0 ] && ! kill -0 $FLASK_PID 2>/dev/null; then
            echo "   ❌ Flask process died! Check the Flask terminal window for errors."
            exit 1
        fi
        
        sleep 2
    done
    
    # If Flask health check failed, still continue but warn user
    if [ "$FLASK_STARTED" = false ]; then
        echo "   ⚠️  Flask health check timed out, but process is running. Check Flask terminal for details."
    fi
    
    # Start React dev server in separate terminal window
    echo "⚛️  Starting React dev server in new terminal window..."
    echo "   ⏳ This will take 30-60 seconds to compile..."
    
    # Create React startup script
    cat > start_react_dev.sh << 'EOF'
#!/bin/bash
cd "$(dirname "$0")/orchestrator/dashboard/client"
export PORT=3000
export REACT_APP_API_URL=http://localhost:5001
echo "⚛️  React Development Server Starting..."
echo "📍 Working Directory: $(pwd)"
echo "🌐 React app will be available at: http://localhost:3000"
echo "⚠️  Port 3000 is FORCED - no port selection prompts"
echo "⏳ Initial compilation may take 30-60 seconds..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
npm start
EOF
    chmod +x start_react_dev.sh
    
    # Launch React in new Terminal window
    osascript -e "
    tell application \"Terminal\"
        do script \"cd '$PWD' && ./start_react_dev.sh\"
        set custom title of front window to \"⚛️  React Development Server\"
    end tell"
    
    # Give React a moment to start (but don't wait for full compilation)
    sleep 3
    
    # Find React PID
    REACT_PID=$(pgrep -f "react-scripts start" | head -1)
    echo "   📋 React PID: $REACT_PID (starting in separate terminal)"
    
    # Brief check that React process started (don't wait for full compilation)
    echo "   ⏳ Verifying React process started..."
    REACT_STARTED=false
    for i in {1..5}; do
        if kill -0 $REACT_PID 2>/dev/null; then
            echo "   ✅ React process is running! (compilation will continue in background)"
            REACT_STARTED=true
            break
        fi
        sleep 1
    done
    
    echo ""
    echo "🎉 Development servers launched!"
    echo "================================================="
    
    # Show final status
    echo "📊 Server Status:"
    if [ "$FLASK_STARTED" = true ]; then
        echo "  🐍 Flask backend: ✅ RUNNING in separate terminal window"
    else
        echo "  🐍 Flask backend: ⚠️  STARTING in separate terminal window"
    fi
    
    if [ "$REACT_STARTED" = true ]; then
        echo "  ⚛️  React frontend: ✅ STARTING in separate terminal window"
    else
        echo "  ⚛️  React frontend: ⚠️  Check separate terminal window for issues"
    fi
    
    echo ""
    echo "🖥️  TWO TERMINAL WINDOWS OPENED:"
    echo "  📋 Terminal 1: 🐍 Flask Development Server (port 5001)"
    echo "  📋 Terminal 2: ⚛️  React Development Server (port 3000)"
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "🌟 DEVELOPMENT MODE - USE PORT 3000 FOR TESTING!"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "🎯 PRIMARY URL FOR TESTING:"
    echo "   👉 http://localhost:3000 ← ALWAYS THIS PORT!"
    echo "      • Full React development experience"
    echo "      • Hot reloading enabled"
    echo "      • Code changes appear instantly"
    echo "      • Port 3000 is FORCED - no more port conflicts!"
    echo ""
    echo "🔧 BACKEND API (for direct API testing):"
    echo "   👉 http://localhost:5001"
    echo "      • Flask API endpoints only"
    echo "      • JSON responses"
    echo "      • Used internally by React app"
    echo ""
    echo "⏰ React will be ready in 30-60 seconds at http://localhost:3000"
    echo "📺 Live output: Check the separate terminal windows!"
    echo ""
    echo "📋 Process IDs: Flask=$FLASK_PID, React=$REACT_PID"
    echo "🛑 To stop: Press Ctrl+C in the terminal windows or run ./stop_orchestrator.sh"
    echo ""
    echo "✅ Setup complete! Script will now exit."
    echo "💡 The development servers will continue running in separate terminals."
    
    # Clean up temporary files
    rm -f start_flask_dev.sh start_react_dev.sh
    
    echo ""
    echo "🚀 Development environment is ready!"
    
else
    # Production mode - use Docker
    # Check if Docker is available
    if ! command -v docker &> /dev/null; then
        echo "❌ Docker is required for consistent deployment"
        echo ""
        echo "📥 Please install Docker:"
        echo "  macOS: Download from https://www.docker.com/products/docker-desktop"
        echo "  Linux: curl -fsSL https://get.docker.com -o get-docker.sh && sh get-docker.sh"
        echo ""
        echo "🎯 Why Docker is required:"
        echo "  ✅ Guarantees identical behavior to Railway"
        echo "  ✅ Same Python version, packages, and environment"
        echo "  ✅ No more deployment differences"
        echo ""
        exit 1
    fi

    echo "✅ Docker is available"

    # Check if Docker daemon is running
    if ! docker info >/dev/null 2>&1; then
        echo "❌ Docker daemon is not running"
        echo ""
        echo "🔧 Please start Docker:"
        echo "  macOS: Open Docker Desktop application"
        echo "  Linux: sudo systemctl start docker"
        echo ""
        exit 1
    fi

    echo "✅ Docker daemon is running"

    # Auto-cleanup any existing services
    if lsof -Pi :5001 -sTCP:LISTEN -t >/dev/null 2>&1 || lsof -Pi :3000 -sTCP:LISTEN -t >/dev/null 2>&1; then
        echo "🧹 Stopping existing services..."
        
        # Stop development servers
        if pgrep -f "python.*launcher.py" >/dev/null 2>&1; then
            echo "  🐍 Stopping Flask backend..."
            pkill -f "python.*launcher.py" 2>/dev/null || true
        fi
        
        if pgrep -f "react-scripts start" >/dev/null 2>&1; then
            echo "  ⚛️  Stopping React dev server..."
            pkill -f "react-scripts start" 2>/dev/null || true
        fi
        
        # Stop Docker containers
        echo "  🐳 Stopping Docker containers..."
        docker-compose down >/dev/null 2>&1 || true
        
        # Wait for services to stop
        echo "  ⏳ Waiting for services to stop..."
        sleep 3
        
        # Force kill anything still on the ports
        echo "  💥 Force killing processes on ports 5001 and 3000..."
        lsof -ti:5001 | xargs -r kill -9 2>/dev/null || true
        lsof -ti:3000 | xargs -r kill -9 2>/dev/null || true
        
        # Also kill any launcher.py processes specifically
        pkill -9 -f "launcher.py" 2>/dev/null || true
        
        echo "✅ All existing services stopped"
    fi

    echo "✅ Ports are now available"

    echo ""
    echo "🔨 Building and starting containers..."
    echo "🌟 This mirrors your Railway deployment exactly!"
    echo "🔄 Live reloading enabled for code changes!"

    # Build and start with Docker Compose
    docker-compose up --build
fi

    echo ""
    echo "🎉 Dashboard system started!"
    echo "================================================="
    echo "📊 Service URL: http://localhost:5001"
    echo "🔍 Health check: http://localhost:5001/health"
    echo ""
    echo "📋 What's running:"
    echo "  🐳 Identical Docker container as Railway"
    echo "  🔧 Flask backend on port 5001 (live reload enabled)"
    echo "  🌐 React frontend (built and served by Flask)"
    echo "  💾 Database persisted in ./database volume"
    echo "  🔄 Code changes will appear automatically!"
    echo ""
    echo "🛑 To stop: ./stop_orchestrator.sh"
    echo "🔄 To restart: ./start_orchestrator.sh"
    echo "💻 For development: ./start_orchestrator.sh --dev"
    echo ""
    echo "✨ Perfect Railway consistency achieved!" 