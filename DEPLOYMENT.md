# 🚀 Dashboard Deployment Guide

This guide shows you how to host your dashboard and restrict access to your team only.

## 🔐 Security Features Added
- **Basic HTTP Authentication**: Login required for all dashboard access
- **Environment-based credentials**: Secure password management
- **SSL/HTTPS support**: Encrypted communication
- **Security headers**: Protection against common attacks
- **IP whitelisting option**: Restrict access by IP address

## 📋 Prerequisites
1. Your dashboard code
2. A domain name (optional but recommended)
3. SSL certificate (Let's Encrypt is free)

## 🌟 Quick Start (Railway - Recommended)

### 1. Prepare your repository
```bash
git add .
git commit -m "Add deployment configuration"
git push origin main
```

### 2. Deploy on Railway
1. Go to [railway.app](https://railway.app)
2. Sign up with GitHub
3. Click "New Project" → "Deploy from GitHub repo"
4. Select your repository
5. Railway will auto-detect your Python app

### 3. Configure environment variables
In Railway dashboard, go to Variables and add:
```
ADMIN_USERNAME=your-admin-username
ADMIN_PASSWORD=your-secure-admin-password
TEAM_USERNAME=your-team-username
TEAM_PASSWORD=your-secure-team-password
SECRET_KEY=your-super-secure-secret-key-change-this
FLASK_ENV=production
```

### 4. Access your dashboard
- Railway will provide a URL like: `https://your-app-name.railway.app`
- Visit the URL and enter your credentials

## 🐳 Docker Deployment

### 1. Build and run locally
```bash
# Build the image
docker build -t dashboard .

# Run with environment variables
docker run -d -p 5001:5001 \
  -e ADMIN_USERNAME=admin \
  -e ADMIN_PASSWORD=secure-password \
  -e TEAM_USERNAME=team \
  -e TEAM_PASSWORD=team-password \
  -e SECRET_KEY=your-secret-key \
  -e FLASK_ENV=production \
  --name dashboard \
  dashboard
```

### 2. Deploy with Docker Compose
```bash
# Edit docker-compose.yml with your credentials
# Then run:
docker-compose up -d
```

## 🌐 VPS Deployment (DigitalOcean, Linode, etc.)

### 1. Set up your server
```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

### 2. Deploy your app
```bash
# Clone your repository
git clone https://github.com/yourusername/your-repo.git
cd your-repo

# Edit docker-compose.yml with your credentials
nano docker-compose.yml

# Deploy
docker-compose up -d
```

### 3. Set up SSL with Let's Encrypt
```bash
# Install certbot
sudo apt install certbot

# Get SSL certificate
sudo certbot certonly --standalone -d your-domain.com

# Copy certificates to your app directory
sudo cp /etc/letsencrypt/live/your-domain.com/fullchain.pem ./ssl/cert.pem
sudo cp /etc/letsencrypt/live/your-domain.com/privkey.pem ./ssl/key.pem
sudo chown $USER:$USER ./ssl/*

# Restart with SSL
docker-compose down
docker-compose up -d
```

## 🔒 Additional Security Options

### Option 1: IP Whitelisting
Add your team's IP addresses to `nginx.conf`:
```nginx
# Uncomment and modify these lines in nginx.conf
allow 203.0.113.0/24;  # Your office IP range
allow 198.51.100.0/24; # Team member's home IP
deny all;
```

### Option 2: VPN Access
- Set up a VPN server (OpenVPN, WireGuard)
- Only allow dashboard access from VPN IPs
- Give team members VPN access

### Option 3: Google OAuth (Advanced)
Replace basic auth with Google OAuth:
```python
# Install: pip install flask-dance
from flask_dance.contrib.google import make_google_blueprint, google

# Add to your app
google_bp = make_google_blueprint(
    client_id="your-google-client-id",
    client_secret="your-google-client-secret",
    scope=["openid", "email", "profile"]
)
app.register_blueprint(google_bp, url_prefix="/login")
```

## 🎯 Team Access Management

### Adding team members
1. **Basic Auth**: Add credentials to environment variables
2. **Google OAuth**: Add email addresses to allowed list
3. **IP Whitelist**: Add their IP addresses

### Managing passwords
```bash
# Generate secure passwords
openssl rand -base64 32

# Update environment variables in your hosting platform
# Restart the application
```

## 🔧 Troubleshooting

### Common issues:
1. **Can't access dashboard**: Check firewall settings
2. **SSL errors**: Verify certificate paths
3. **Authentication fails**: Check environment variables
4. **App won't start**: Check logs with `docker-compose logs`

### Health check:
```bash
# Check if app is running
curl -u admin:password https://your-domain.com/api/analytics-pipeline/health
```

## 📊 Monitoring

### Basic monitoring:
```bash
# Check container status
docker ps

# View logs
docker-compose logs -f dashboard

# Monitor resources
docker stats
```

### Advanced monitoring:
- Add [Uptime Robot](https://uptimerobot.com) for uptime monitoring
- Use [Sentry](https://sentry.io) for error tracking
- Set up [Grafana](https://grafana.com) for performance metrics

## 💰 Cost Estimates

| Platform | Monthly Cost | Pros | Cons |
|----------|-------------|------|------|
| Railway | $5-20 | Easy setup, auto-scaling | Limited storage |
| Heroku | $7-25 | Popular, many addons | More expensive |
| DigitalOcean | $5-10 | Full control, cheap | More setup required |
| AWS/GCP | $5-50 | Enterprise features | Complex pricing |

## 🚀 Next Steps

1. Choose your hosting platform
2. Set up authentication credentials
3. Configure SSL certificate
4. Test with your team
5. Set up monitoring
6. Create backup strategy

Need help? The deployment is now ready with basic HTTP authentication protecting your dashboard! 