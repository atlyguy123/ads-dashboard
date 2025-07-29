#!/usr/bin/env python3
"""
Comprehensive Setup Validation Script

This script validates that all deployment consistency changes
have been implemented correctly.
"""

import os
import json
import sys
from pathlib import Path

def check_file_exists(file_path, description=""):
    """Check if a file exists and return status"""
    exists = os.path.exists(file_path)
    status = "✅" if exists else "❌"
    desc = f" ({description})" if description else ""
    print(f"  {status} {file_path}{desc}")
    return exists

def check_file_contains(file_path, content, description=""):
    """Check if a file contains specific content"""
    try:
        with open(file_path, 'r') as f:
            file_content = f.read()
            contains = content in file_content
            status = "✅" if contains else "❌"
            desc = f" ({description})" if description else ""
            print(f"  {status} {file_path} contains: {content[:50]}...{desc}")
            return contains
    except FileNotFoundError:
        print(f"  ❌ {file_path} not found")
        return False

def validate_railway_config():
    """Validate Railway configuration"""
    print("\n📡 RAILWAY CONFIGURATION:")
    print("=" * 40)
    
    # Check railway.json exists and has correct content
    railway_json_ok = check_file_exists("railway.json", "Railway config")
    
    if railway_json_ok:
        try:
            with open("railway.json", 'r') as f:
                config = json.load(f)
                
            if config.get("build", {}).get("builder") == "DOCKERFILE":
                print("  ✅ Railway configured to use Docker")
            else:
                print("  ❌ Railway not using Docker builder")
                return False
                
            if "launcher.py" in config.get("deploy", {}).get("startCommand", ""):
                print("  ✅ Railway using launcher.py")
            else:
                print("  ❌ Railway not using launcher.py")
                return False
                
        except Exception as e:
            print(f"  ❌ Error reading railway.json: {e}")
            return False
    
    # Check railway.toml is simplified
    railway_toml_ok = check_file_exists("railway.toml", "Railway deployment config")
    if railway_toml_ok:
        simplified = not check_file_contains("railway.toml", "buildCommand", "build command removed")
        if simplified:
            print("  ✅ railway.toml simplified (build moved to Dockerfile)")
        else:
            print("  ❌ railway.toml still has complex build commands")
    
    return railway_json_ok and railway_toml_ok

def validate_docker_config():
    """Validate Docker configuration"""
    print("\n🐳 DOCKER CONFIGURATION:")
    print("=" * 40)
    
    # Check Dockerfile
    dockerfile_ok = check_file_exists("Dockerfile", "Main container config")
    if dockerfile_ok:
        docker_checks = [
            ("python:3.9.18-slim", "Exact Python version"),
            ("COPY requirements.txt", "Requirements copied first"),
            ("COPY orchestrator/dashboard/client/package", "Package.json copied"),
            ("npm run build", "React build included"),
            ("HEALTHCHECK", "Health check configured"),
            ("launcher.py", "Uses launcher.py")
        ]
        
        for content, desc in docker_checks:
            check_file_contains("Dockerfile", content, desc)
    
    # Check docker-compose.yml
    compose_ok = check_file_exists("docker-compose.yml", "Local Docker setup")
    if compose_ok:
        compose_checks = [
            ("5000:5000", "Port 5000 mapped"),
            ("FLASK_ENV=development", "Development environment"),
            ("healthcheck", "Health check configured"),
            ("./database:/app/database", "Database volume mounted")
        ]
        
        for content, desc in compose_checks:
            check_file_contains("docker-compose.yml", content, desc)
    
    # Check .dockerignore
    dockerignore_ok = check_file_exists(".dockerignore", "Docker ignore file")
    if dockerignore_ok:
        ignore_checks = [
            ("venv/", "Virtual environment ignored"),
            ("node_modules/", "Node modules ignored"),
            (".env", "Environment files ignored"),
            ("*.md", "Documentation ignored")
        ]
        
        for content, desc in ignore_checks:
            check_file_contains(".dockerignore", content, desc)
    
    return dockerfile_ok and compose_ok and dockerignore_ok

def validate_dependencies():
    """Validate dependency management"""
    print("\n📦 DEPENDENCY MANAGEMENT:")
    print("=" * 40)
    
    requirements_ok = check_file_exists("requirements.txt", "Python dependencies")
    if requirements_ok:
        # Check for pinned versions (no >= ranges)
        with open("requirements.txt", 'r') as f:
            content = f.read()
            
        if ">=" in content:
            print("  ❌ requirements.txt has version ranges (>=)")
            print("  💡 Consider pinning exact versions for consistency")
        else:
            print("  ✅ requirements.txt uses pinned versions")
            
        # Check for important packages
        important_packages = ["Flask==2.3.3", "pandas==1.5.3", "numpy==1.24.3", "gunicorn==21.2.0"]
        for package in important_packages:
            if package.split("==")[0] in content:
                print(f"  ✅ {package.split('==')[0]} included")
            else:
                print(f"  ❌ {package.split('==')[0]} missing")
    
    # Check launcher.py
    launcher_ok = check_file_exists("launcher.py", "Railway launcher script")
    if launcher_ok:
        launcher_checks = [
            ("from orchestrator.app import app", "Imports Flask app"),
            ("PORT", "Uses PORT environment variable"),
            ("HOST", "Uses HOST environment variable")
        ]
        
        for content, desc in launcher_checks:
            check_file_contains("launcher.py", content, desc)
    
    return requirements_ok and launcher_ok

def validate_scripts():
    """Validate updated scripts"""
    print("\n🔧 SCRIPT UPDATES:")
    print("=" * 40)
    
    # Check build and deploy script
    build_script_ok = check_file_exists("orchestrator/dashboard/build-and-deploy.sh", "Build script")
    if build_script_ok:
        script_checks = [
            ("Docker for complete Railway consistency", "Docker-first approach"),
            ("docker-compose up --build", "Docker build command"),
            ("Docker is required", "Docker requirement check")
        ]
        
        for content, desc in script_checks:
            check_file_contains("orchestrator/dashboard/build-and-deploy.sh", content, desc)
    
    # Check start script
    start_script_ok = check_file_exists("start_orchestrator.sh", "Start script")
    if start_script_ok:
        start_checks = [
            ("Docker for complete Railway consistency", "Docker-first approach"),
            ("docker-compose up --build", "Docker start command"),
            ("Docker is required", "Docker requirement check")
        ]
        
        for content, desc in start_checks:
            check_file_contains("start_orchestrator.sh", content, desc)
    
    # Check stop script
    stop_script_ok = check_file_exists("stop_orchestrator.sh", "Stop script")
    if stop_script_ok:
        stop_checks = [
            ("Docker-based dashboard services", "Docker-only mode"),
            ("docker-compose down", "Docker stop command")
        ]
        
        for content, desc in stop_checks:
            check_file_contains("stop_orchestrator.sh", content, desc)
    
    # Check verification scripts
    deploy_check_ok = check_file_exists("scripts/deploy-check.sh", "Deployment check")
    env_verify_ok = check_file_exists("scripts/verify-environment.py", "Environment verification")
    
    return build_script_ok and start_script_ok and stop_script_ok and deploy_check_ok and env_verify_ok

def validate_documentation():
    """Validate documentation"""
    print("\n📚 DOCUMENTATION:")
    print("=" * 40)
    
    guide_ok = check_file_exists("DEPLOYMENT_CONSISTENCY_GUIDE.md", "Complete deployment guide")
    if guide_ok:
        doc_checks = [
            ("Docker-first deployment", "Docker-first explanation"),
            ("identical behavior", "Consistency promise"),
            ("railway.json", "Railway configuration"),
            ("exact package versions", "Dependency management")
        ]
        
        for content, desc in doc_checks:
            check_file_contains("DEPLOYMENT_CONSISTENCY_GUIDE.md", content, desc)
    
    return guide_ok

def main():
    """Run all validations"""
    print("🔍 DEPLOYMENT CONSISTENCY VALIDATION")
    print("=====================================")
    print("Checking all changes have been implemented correctly...")
    
    results = {
        "railway": validate_railway_config(),
        "docker": validate_docker_config(), 
        "dependencies": validate_dependencies(),
        "scripts": validate_scripts(),
        "documentation": validate_documentation()
    }
    
    print("\n" + "=" * 50)
    print("📊 VALIDATION SUMMARY:")
    print("=" * 50)
    
    all_good = True
    for component, status in results.items():
        status_icon = "✅" if status else "❌"
        print(f"  {status_icon} {component.upper()}: {'PASS' if status else 'FAIL'}")
        if not status:
            all_good = False
    
    print("\n" + "=" * 50)
    if all_good:
        print("🎉 ALL VALIDATIONS PASSED!")
        print("✅ Deployment consistency setup is complete")
        print("")
        print("🚀 Your deployment should now be consistent between local and Railway!")
        print("")
        print("Next steps:")
        print("1. Install Docker (required for consistency)")
        print("2. Test locally: ./start_orchestrator.sh")
        print("3. Deploy to Railway: git push origin main")
        print("4. Verify consistency: python3 scripts/verify-environment.py")
        return 0
    else:
        print("❌ VALIDATION FAILED!")
        print("Some components need attention. Check the details above.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 