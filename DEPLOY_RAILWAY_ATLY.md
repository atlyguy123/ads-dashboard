# 🚀 Deploy ATLY Dashboard on Railway (No Payment Required)

Railway is easier than Heroku - no payment verification needed initially!

## ⚡ Quick Railway Deployment:

### **1. Push to GitHub first:**
```bash
# Add all files and commit
git add .
git commit -m "Deploy ATLY Marketing Dashboard"
git push origin main
```

### **2. Deploy on Railway:**
1. Go to [railway.app](https://railway.app)
2. Sign up with GitHub
3. Click "New Project" → "Deploy from GitHub repo"
4. Select your repository
5. Railway auto-detects Python and builds automatically

### **3. Set Environment Variables:**
In Railway dashboard, go to Variables tab and add:
```
ADMIN_USERNAME=admin
ADMIN_PASSWORD=AtlySecure2024!
TEAM_USERNAME=team
TEAM_PASSWORD=AtlyTeam2024!
SECRET_KEY=your-super-secure-secret-key
FLASK_ENV=production
```

### **4. Your Dashboard URLs:**
Railway will give you a URL like:
- **Main**: `https://your-app-name.railway.app`
- **Dashboard**: `https://your-app-name.railway.app/ads-dashboard`

## ✅ Advantages of Railway:
- ✅ No payment verification required
- ✅ Auto-deploys from GitHub
- ✅ Built-in database support
- ✅ Simple environment variable management
- ✅ Background processes included
- ✅ Free tier: 500 hours/month

## 🔄 Background Jobs:
Railway automatically runs both:
- Web process (dashboard)
- Worker process (background jobs)

For daily scheduling, add Railway Cron:
- Go to Railway dashboard
- Add Cron job: `cd orchestrator && python daily_scheduler.py`
- Schedule: `0 7 * * *` (7 AM UTC = 9 AM Israel time)

**Want me to help you with Railway instead?** 