#!/bin/bash

# Build and Deploy Script for Ads Dashboard
# Usage: ./build-and-deploy.sh

set -e  # Exit on any error

echo "🚀 Building and deploying Ads Dashboard..."
echo "============================================"

# Navigate to client directory
cd client

echo "📦 Installing/updating dependencies..."
npm install

echo "🔨 Building React application..."
npm run build

echo "📁 Copying build files to static directory..."
cd ..
rm -rf static/*
cp -r client/build/* static/

echo "✅ Dashboard built and deployed successfully!"
echo ""
echo "🌐 Access the dashboard at: http://localhost:5001/ads-dashboard"
echo "⚙️  Make sure the orchestrator is running with: python app.py"
echo ""
echo "For development mode:"
echo "  cd client && npm start"
echo "" 