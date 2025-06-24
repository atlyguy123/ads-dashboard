# 🚀 Deploy ATLY Marketing Dashboard

## Your Specific Configuration:
- **App Name**: `atly-marketing-dashboard`
- **URL**: `https://atly-marketing-dashboard.herokuapp.com`
- **Schedule**: Daily at 9 AM Israel time (7 AM UTC)
- **Database**: Using existing SQLite database

## ⚡ Deploy Commands (Copy & Paste):

```bash
# 1. Login to Heroku
heroku login

# 2. Create ATLY app
heroku create atly-marketing-dashboard

# 3. Add buildpacks
heroku buildpacks:add heroku/python
heroku buildpacks:add heroku/nodejs

# 4. Set your environment variables
heroku config:set ADMIN_USERNAME=admin
heroku config:set ADMIN_PASSWORD=AtlySecure2024!
heroku config:set TEAM_USERNAME=team
heroku config:set TEAM_PASSWORD=AtlyTeam2024!
heroku config:set SECRET_KEY=$(openssl rand -base64 32)
heroku config:set FLASK_ENV=production
heroku config:set HEROKU_APP_NAME=atly-marketing-dashboard

# 5. Deploy your app
git add .
git commit -m "Deploy ATLY Marketing Dashboard to Heroku"
git push heroku main

# 6. Scale processes for 24/7 operation
heroku ps:scale web=1 worker=1

# 7. Add scheduler for daily 9 AM Israel time runs
heroku addons:create scheduler:standard
heroku addons:open scheduler
```

## 📅 **Scheduler Configuration:**
When the scheduler dashboard opens:
1. Click "Create job"
2. **Command**: `cd orchestrator && python daily_scheduler.py`
3. **Frequency**: `Daily at 07:00 UTC` (= 9 AM Israel time)
4. **Save**

## 🌐 **Your Live Dashboard URLs:**
- **Main**: https://atly-marketing-dashboard.herokuapp.com
- **Dashboard**: https://atly-marketing-dashboard.herokuapp.com/ads-dashboard
- **Pipelines**: https://atly-marketing-dashboard.herokuapp.com/pipelines
- **Debug**: https://atly-marketing-dashboard.herokuapp.com/debug

## 🔐 **Team Access Credentials:**
- **Admin**: `admin` / `AtlySecure2024!`
- **Team**: `team` / `AtlyTeam2024!`

## 💾 **Database Setup:**
Since your database files are too large for Heroku (1.4GB+), the app will:
- **Schema**: Upload your `database/schema.sql` file
- **Auto-create**: Empty databases created automatically from schema on first run
- **Data**: Pipeline will populate databases with fresh data daily
- **Note**: First run will take longer as it builds from scratch

## 🔄 **What Runs Automatically:**
- **Daily at 9 AM Israel time**: Complete master pipeline
  - Mixpanel data download & processing
  - Pre-processing pipeline
  - Meta analytics update
- **Background worker**: Monitors and handles jobs 24/7
- **Web dashboard**: Always available for team access

## ✅ **Deployment Checklist:**
- [ ] Run the commands above
- [ ] Set up scheduler job (7:00 UTC daily)
- [ ] Test dashboard access
- [ ] Share credentials with team
- [ ] Monitor first pipeline run

## 🔧 **Monitor Your App:**
```bash
# View all logs
heroku logs --tail

# Check processes
heroku ps

# Check worker status
heroku logs --tail --ps worker

# Test pipeline manually
heroku run cd orchestrator && python daily_scheduler.py
```

## 📊 **Expected Behavior:**
1. **Dashboard**: Accessible 24/7 at your URL
2. **Authentication**: Team login required
3. **Daily Pipeline**: Runs automatically at 9 AM Israel time
4. **Background Jobs**: Process data continuously
5. **Logs**: Track all activities and errors

Ready to deploy? Just run the commands above! 🚀 