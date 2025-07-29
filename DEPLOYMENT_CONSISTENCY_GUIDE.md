# Deployment Consistency Guide

## Overview

This project uses **Docker-first deployment** to ensure **identical behavior** between your local environment and Railway deployment. No more deployment differences!

## Problem Solved

**Before**: Different results between local development and Railway production due to:
- ❌ Different Python/package versions
- ❌ Different build processes  
- ❌ Different file system behaviors
- ❌ Different environment configurations

**After**: **Identical behavior everywhere** using Docker:
- ✅ Same OS environment (Python 3.9.18-slim)
- ✅ Same package versions (pinned exactly)
- ✅ Same build process (React + Python in Dockerfile)
- ✅ Same runtime environment **always**

## How It Works

### 🐳 Docker-First Approach

**Local Development**: Always uses Docker containers  
**Railway Production**: Uses identical Docker containers  
**Result**: 100% consistency guaranteed

**Key Files**:
- `Dockerfile` - Defines identical environment everywhere
- `docker-compose.yml` - Local development setup
- `railway.json` - Railway uses same Dockerfile
- `requirements.txt` - Exact package versions

### 🎯 Perfect Alignment

```
Local Docker Container    ←→    Railway Docker Container
   (Identical Build)             (Identical Build)
        ↓                              ↓
   Same Results                   Same Results
```

## Usage

### Local Development

```bash
# Start dashboard (always uses Docker)
./start_orchestrator.sh

# Stop dashboard
./stop_orchestrator.sh

# Build and deploy
cd orchestrator/dashboard && ./build-and-deploy.sh
```

**URLs**:
- Dashboard: http://localhost:5000
- Health check: http://localhost:5000/health

### Railway Deployment

```bash
# Deploy (uses identical Docker setup)
git push origin main

# Railway automatically:
# 1. Builds using same Dockerfile
# 2. Runs identical container
# 3. Uses same launcher.py
```

### Verification

```bash
# Check deployment readiness
./scripts/deploy-check.sh

# Validate setup
python3 scripts/validate-setup.py

# Compare environments
python3 scripts/verify-environment.py
```

## Docker Requirements

**Install Docker** (required):
- **macOS**: [Docker Desktop](https://www.docker.com/products/docker-desktop)
- **Linux**: `curl -fsSL https://get.docker.com -o get-docker.sh && sh get-docker.sh`

**Why Docker is required**:
- ✅ Guarantees identical behavior to Railway
- ✅ Eliminates "works on my machine" issues
- ✅ Professional deployment standard
- ✅ Perfect environment consistency

## Environment Variables

Set these in Railway dashboard:

**Required**:
- `META_ACCESS_TOKEN`
- `META_ACCOUNT_ID` 
- `SECRET_KEY`

**Optional**:
- `DEFAULT_TIMEZONE` (default: America/New_York)
- `FLASK_ENV` (set to "production" on Railway)

## File Structure

```
📁 Project Root
├── 🐳 Dockerfile              # Main container definition
├── 🐳 docker-compose.yml      # Local development
├── 🚂 railway.json            # Railway Docker config
├── 📦 requirements.txt        # Pinned dependencies
├── 🚀 launcher.py             # Unified startup
├── 🔧 start_orchestrator.sh   # Local startup (Docker)
├── 🛑 stop_orchestrator.sh    # Local shutdown
└── 📚 DEPLOYMENT_CONSISTENCY_GUIDE.md
```

## Troubleshooting

### Docker Issues

```bash
# Check Docker is running
docker info

# Check containers
docker-compose ps

# View logs
docker-compose logs -f

# Restart containers
./stop_orchestrator.sh && ./start_orchestrator.sh
```

### Port Conflicts

```bash
# Check what's using port 5000
lsof -i :5000

# Stop containers
./stop_orchestrator.sh
```

### Build Issues

```bash
# Clean rebuild
docker-compose down
docker-compose up --build --force-recreate
```

## Best Practices

1. **Always use Docker locally** - guarantees Railway consistency
2. **Test with Docker before deploying** - `./start_orchestrator.sh`
3. **Verify deployment** - `./scripts/deploy-check.sh`
4. **Check health endpoints** - `/health` and `/api/analytics-pipeline/health`

## Migration Benefits

- ✅ **Zero deployment differences** - Docker containers are identical
- ✅ **Simplified development** - one command to start everything
- ✅ **Professional workflow** - industry standard practices
- ✅ **Reliable deployments** - if it works locally, it works on Railway

This Docker-first approach guarantees **perfect consistency** between local development and Railway production. No more surprises! 🎉 