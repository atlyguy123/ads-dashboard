# 🚀 Heroku Deployment Guide

Complete guide to deploy your analytics dashboard to Heroku with background job processing.

## 📋 Prerequisites

1. [Heroku CLI](https://devcenter.heroku.com/articles/heroku-cli) installed
2. Git repository with your code
3. Heroku account

## 🚀 Step-by-Step Deployment

### 1. **Login to Heroku**
```bash
heroku login
```

### 2. **Create Heroku App**
```bash
# Create app (replace 'your-dashboard-name' with your preferred name)
heroku create your-dashboard-name

# Or create in a specific region
heroku create your-dashboard-name --region us

# Note: The app name will be your public URL: https://your-dashboard-name.herokuapp.com
```

### 3. **Add Buildpacks** (Multi-language support)
```bash
# Add Python buildpack (for your Flask backend)
heroku buildpacks:add heroku/python

# Add Node.js buildpack (for React frontend build)
heroku buildpacks:add heroku/nodejs
```

### 4. **Set Environment Variables**
```bash
# Authentication credentials (CHANGE THESE!)
heroku config:set ADMIN_USERNAME=your-admin-username
heroku config:set ADMIN_PASSWORD=your-secure-admin-password
heroku config:set TEAM_USERNAME=team
heroku config:set TEAM_PASSWORD=your-team-secure-password

# App configuration
heroku config:set SECRET_KEY=$(openssl rand -base64 32)
heroku config:set FLASK_ENV=production
heroku config:set HEROKU_APP_NAME=your-dashboard-name

# Optional: Database URL (Heroku will auto-add if you add Postgres)
# heroku config:set DATABASE_URL=your-database-url
```

### 5. **Add Database** (Optional but recommended)
```bash
# Add Heroku Postgres (free tier)
heroku addons:create heroku-postgresql:mini

# Or hobby tier for more storage
# heroku addons:create heroku-postgresql:basic
```

### 6. **Deploy Your App**
```bash
# Add and commit your changes
git add .
git commit -m "Configure for Heroku deployment"

# Deploy to Heroku
git push heroku main

# Or if you're on a different branch:
# git push heroku your-branch:main
```

### 7. **Scale Your App**
```bash
# Scale web process
heroku ps:scale web=1

# Enable background worker (IMPORTANT for daily jobs)
heroku ps:scale worker=1
```

### 8. **Set Up Daily Scheduler**
```bash
# Add Heroku Scheduler (free add-on)
heroku addons:create scheduler:standard

# Open scheduler dashboard
heroku addons:open scheduler
```

**In the Scheduler dashboard:**
1. Click "Create job"
2. Set command: `cd orchestrator && python daily_scheduler.py`
3. Set frequency: `Daily at 2:00 AM UTC` (or your preferred time)
4. Save the job

## 🌐 Access Your Dashboard

Your dashboard will be available at:
- **URL**: `https://your-dashboard-name.herokuapp.com`
- **Dashboard**: `https://your-dashboard-name.herokuapp.com/ads-dashboard`
- **Pipelines**: `https://your-dashboard-name.herokuapp.com/pipelines`
- **Debug**: `https://your-dashboard-name.herokuapp.com/debug`

## 🔧 Post-Deployment Configuration

### **View Logs**
```bash
# View app logs
heroku logs --tail

# View worker logs specifically
heroku logs --tail --ps worker

# View scheduler logs
heroku logs --tail --ps scheduler
```

### **Check App Status**
```bash
# Check running processes
heroku ps

# Check app info
heroku info

# Check config vars
heroku config
```

### **Manage Processes**
```bash
# Restart the app
heroku restart

# Scale processes
heroku ps:scale web=1 worker=1

# Stop worker temporarily
heroku ps:scale worker=0
```

## 🔐 Team Access Setup

### **Share with Team**
1. Give team members the URL: `https://your-dashboard-name.herokuapp.com`
2. Share credentials:
   - **Admin**: `your-admin-username` / `your-secure-admin-password`
   - **Team**: `team` / `your-team-secure-password`

### **Add Team Collaborators** (Optional)
```bash
# Add team members as Heroku collaborators
heroku access:add teammate@company.com
```

## 🔄 Background Jobs Configuration

Your app now has **3 processes**:

1. **Web Process** (`web`): Serves the dashboard and API
2. **Worker Process** (`worker`): Runs continuous background monitoring
3. **Scheduler** (via add-on): Runs daily pipeline at scheduled time

### **Worker Process Features:**
- ✅ Runs independently of web traffic
- ✅ Monitors for daily job triggers
- ✅ Health checks every hour
- ✅ Graceful shutdown handling
- ✅ Persistent state tracking

### **Daily Pipeline Schedule:**
- Runs automatically via Heroku Scheduler
- Executes master pipeline (Mixpanel → Pre-processing → Meta)
- Logs all activities
- Handles failures gracefully

## 📊 Monitoring & Maintenance

### **Check Background Jobs**
```bash
# Check if worker is running
heroku ps:info worker

# View worker logs
heroku logs --tail --ps worker

# Check scheduler jobs
heroku addons:open scheduler
```

### **Health Checks**
```bash
# Test API health
curl -u admin:password https://your-dashboard-name.herokuapp.com/api/analytics-pipeline/health

# Check last pipeline run
heroku logs --tail | grep "master pipeline"
```

### **Database Management**
```bash
# Connect to database
heroku pg:psql

# View database info
heroku pg:info

# Create database backup
heroku pg:backups:capture
```

## 💰 Cost Breakdown

| Component | Free Tier | Paid Tier |
|-----------|-----------|-----------|
| **Web Dyno** | 550 hours/month | $7/month (24/7) |
| **Worker Dyno** | 550 hours/month | $7/month (24/7) |
| **Postgres** | 10k rows | $9/month (basic) |
| **Scheduler** | Free | Free |
| **Total** | ~$0-3/month | ~$23/month |

**Note**: Free tier dynos sleep after 30 minutes of inactivity. For production, upgrade to paid dynos.

## 🔧 Troubleshooting

### **Common Issues:**

**1. Build Failed**
```bash
# Check build logs
heroku logs --tail | grep BUILD

# Common fix: Clear build cache
heroku repo:purge_cache
```

**2. Worker Not Running**
```bash
# Check worker status
heroku ps:info worker

# Restart worker
heroku restart worker
```

**3. Frontend Not Loading**
```bash
# Check if build completed
heroku logs --tail | grep "Frontend build complete"

# Manually trigger build
heroku run bash
# Then run: ./heroku-build.sh
```

**4. Authentication Issues**
```bash
# Check environment variables
heroku config | grep USERNAME

# Reset auth credentials
heroku config:set ADMIN_PASSWORD=new-secure-password
```

### **Debug Commands:**
```bash
# Access app shell
heroku run bash

# Check file structure
heroku run ls -la orchestrator/dashboard/static/

# Test pipeline manually
heroku run cd orchestrator && python daily_scheduler.py
```

## 🚀 Go Live Checklist

- [ ] App deployed successfully
- [ ] Web process running
- [ ] Worker process running
- [ ] Daily scheduler configured
- [ ] Environment variables set
- [ ] Team credentials shared
- [ ] Database configured (if needed)
- [ ] SSL certificate active (automatic with Heroku)
- [ ] Logs monitoring set up

## 🔄 Updates & Maintenance

### **Deploy Updates**
```bash
git add .
git commit -m "Update dashboard"
git push heroku main
```

### **Database Migrations**
```bash
# If you add database migrations
heroku run cd orchestrator && python migrate.py
```

### **Scaling for Growth**
```bash
# Scale up for more traffic
heroku ps:scale web=2

# Add more worker processes
heroku ps:scale worker=2
```

---

## 🎉 Your Dashboard is Live!

**URL**: `https://your-dashboard-name.herokuapp.com`

**Features:**
- ✅ Public web access with authentication
- ✅ Background jobs running 24/7
- ✅ Daily pipeline automation
- ✅ Team access control
- ✅ Real-time monitoring
- ✅ Automatic SSL/HTTPS

**Need help?** Check logs with `heroku logs --tail` or contact support! 