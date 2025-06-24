#!/usr/bin/env python3
"""
Deploy environment variables from .env file to Heroku
This script reads all variables from orchestrator/.env and sets them as Heroku config vars
"""

import os
import sys
import subprocess
from pathlib import Path

def load_env_file(env_path):
    """Load environment variables from .env file"""
    env_vars = {}
    
    if not env_path.exists():
        print(f"❌ .env file not found at: {env_path}")
        return env_vars
    
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            # Skip comments and empty lines
            if not line or line.startswith('#'):
                continue
            
            # Parse KEY=VALUE format
            if '=' in line:
                key, value = line.split('=', 1)
                # Remove quotes if present
                value = value.strip('"\'')
                env_vars[key.strip()] = value
    
    return env_vars

def set_heroku_config_vars(app_name, env_vars):
    """Set environment variables as Heroku config vars"""
    print(f"🚀 Setting environment variables for Heroku app: {app_name}")
    print("=" * 60)
    
    success_count = 0
    failed_vars = []
    
    for key, value in env_vars.items():
        try:
            cmd = ['heroku', 'config:set', f'{key}={value}', '--app', app_name]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"✅ {key}: Set successfully")
                success_count += 1
            else:
                print(f"❌ {key}: Failed - {result.stderr.strip()}")
                failed_vars.append(key)
                
        except Exception as e:
            print(f"❌ {key}: Error - {e}")
            failed_vars.append(key)
    
    print("\n" + "=" * 60)
    print(f"📊 Results: {success_count}/{len(env_vars)} variables set successfully")
    
    if failed_vars:
        print(f"❌ Failed variables: {', '.join(failed_vars)}")
        return False
    else:
        print("✅ All environment variables deployed successfully!")
        return True

def main():
    if len(sys.argv) != 2:
        print("Usage: python deploy_env_to_heroku.py <heroku-app-name>")
        print("Example: python deploy_env_to_heroku.py atly-analytics-dashboard")
        sys.exit(1)
    
    app_name = sys.argv[1]
    env_path = Path('orchestrator/.env')
    
    print(f"🔧 ATLY Environment Variable Deployment")
    print(f"App: {app_name}")
    print(f"Env file: {env_path}")
    print()
    
    # Load environment variables
    env_vars = load_env_file(env_path)
    
    if not env_vars:
        print("❌ No environment variables found to deploy")
        sys.exit(1)
    
    print(f"📦 Found {len(env_vars)} environment variables:")
    for key in env_vars.keys():
        # Mask sensitive values
        if any(sensitive in key.lower() for sensitive in ['password', 'secret', 'token', 'key']):
            print(f"   {key}=***MASKED***")
        else:
            print(f"   {key}={env_vars[key]}")
    print()
    
    # Set Heroku config vars
    success = set_heroku_config_vars(app_name, env_vars)
    
    if success:
        print("\n🎉 All environment variables successfully deployed to Heroku!")
        print(f"🌐 Your app: https://{app_name}.herokuapp.com")
        sys.exit(0)
    else:
        print("\n❌ Some environment variables failed to deploy")
        sys.exit(1)

if __name__ == '__main__':
    main() 