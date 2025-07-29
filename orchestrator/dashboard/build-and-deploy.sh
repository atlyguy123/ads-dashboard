#!/bin/bash

# Build and Deploy Script for Ads Dashboard
# Always uses Docker for Railway consistency

set -e  # Exit on any error

echo "🚀 Building and deploying Ads Dashboard with Docker..."
echo "🐳 Using Docker for complete Railway consistency"
echo "============================================"

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
    echo "  ✅ Same build process as production"
    echo "  ✅ No environment differences"
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

echo ""
echo "🔨 Building with Docker (identical to Railway)..."

# Build and start in detached mode
docker-compose up --build -d

echo ""
echo "✅ Dashboard built and deployed with Docker!"
echo "============================================"
echo "🌐 Access the dashboard at: http://localhost:5000"
echo "🔍 Health check: http://localhost:5000/health"
echo ""
echo "📊 What's running:"
echo "  🐳 Identical Docker container as Railway"
echo "  🔧 Flask backend with built React frontend"
echo "  💾 Database persisted in ./database volume"
echo ""
echo "🔄 To see logs: docker-compose logs -f"
echo "🛑 To stop: ./stop_orchestrator.sh"
echo "📋 To check status: docker-compose ps"
echo ""
echo "🎯 Perfect Railway deployment consistency!" 