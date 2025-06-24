#!/bin/bash
# Heroku build script for full-stack deployment

set -e  # Exit on any error

echo "🚀 HEROKU BUILD: Starting build process..."

# Build the React frontend
echo "📦 Building React frontend..."
cd orchestrator/dashboard/client

# Install dependencies
echo "Installing Node.js dependencies..."
npm ci --only=production

# Set production API URL to current Heroku app
if [ -n "$HEROKU_APP_NAME" ]; then
    export REACT_APP_API_URL="https://$HEROKU_APP_NAME.herokuapp.com"
else
    export REACT_APP_API_URL="/"
fi

echo "Building with API URL: $REACT_APP_API_URL"

# Build the React app
npm run build

# Copy build files to static directory
echo "📁 Copying build files to static directory..."
cd ..
rm -rf static/*
cp -r client/build/* static/

echo "✅ HEROKU BUILD: Frontend build complete!"

# Navigate back to root
cd ../../..
echo "🎯 Build process finished - ready for deployment!" 