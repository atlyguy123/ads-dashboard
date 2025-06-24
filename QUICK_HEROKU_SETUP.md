# ⚡ Quick Heroku Setup

## 🚀 Deploy in 5 Minutes

### **Prerequisites:**
- Heroku CLI installed
- Git repository ready

### **Commands to Run:**

```bash
# 1. Login to Heroku
heroku login

# 2. Create your app (replace 'your-dashboard-name')
heroku create your-dashboard-name

# 3. Add buildpacks
heroku buildpacks:add heroku/python
heroku buildpacks:add heroku/nodejs

# 4. Set authentication (CHANGE THESE PASSWORDS!)
heroku config:set ADMIN_USERNAME=admin
heroku config:set ADMIN_PASSWORD=your-secure-admin-password
heroku config:set TEAM_USERNAME=team
heroku config:set TEAM_PASSWORD=your-team-secure-password
heroku config:set SECRET_KEY=$(openssl rand -base64 32)
heroku config:set FLASK_ENV=production
heroku config:set HEROKU_APP_NAME=your-dashboard-name

# 5. Add database (optional)
heroku addons:create heroku-postgresql:mini

# 6. Deploy
git add .
git commit -m "Deploy to Heroku"
git push heroku main

# 7. Scale processes
heroku ps:scale web=1 worker=1

# 8. Add daily scheduler
heroku addons:create scheduler:standard
heroku addons:open scheduler
```

### **In Scheduler Dashboard:**
1. Click "Create job"
2. Command: `cd orchestrator && python daily_scheduler.py`
3. Frequency: `Daily at 2:00 AM UTC`
4. Save

## 🎯 Your Dashboard URLs:
- **Main**: `https://your-dashboard-name.herokuapp.com`
- **Dashboard**: `https://your-dashboard-name.herokuapp.com/ads-dashboard`
- **Pipelines**: `https://your-dashboard-name.herokuapp.com/pipelines`

## 🔐 Team Access:
- **Admin**: `admin` / `your-secure-admin-password`
- **Team**: `team` / `your-team-secure-password`

## ✅ What You Get:
- ✅ Public dashboard with authentication
- ✅ Background worker running 24/7
- ✅ Daily pipeline automation
- ✅ HTTPS enabled automatically
- ✅ Team access control

**Need detailed help?** See `HEROKU_DEPLOYMENT.md` 