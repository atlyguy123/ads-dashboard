#!/usr/bin/env python3
"""
Environment Verification Script

This script helps verify that your local and Railway environments
are consistent by checking versions, paths, and configurations.
"""

import sys
import os
import platform
import json
from datetime import datetime

def get_environment_info():
    """Collect comprehensive environment information"""
    
    env_info = {
        "timestamp": datetime.now().isoformat(),
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "version": platform.version(),
            "machine": platform.machine(),
            "processor": platform.processor(),
        },
        "python": {
            "version": sys.version,
            "executable": sys.executable,
            "path": sys.path[:3],  # First 3 entries
        },
        "environment_vars": {
            "FLASK_ENV": os.environ.get("FLASK_ENV", "not_set"),
            "HOST": os.environ.get("HOST", "not_set"),
            "PORT": os.environ.get("PORT", "not_set"),
            "DEFAULT_TIMEZONE": os.environ.get("DEFAULT_TIMEZONE", "not_set"),
            "RAILWAY_ENVIRONMENT": os.environ.get("RAILWAY_ENVIRONMENT", "not_set"),
            "PYTHONPATH": os.environ.get("PYTHONPATH", "not_set"),
        },
        "working_directory": os.getcwd(),
        "file_checks": {},
        "package_versions": {}
    }
    
    # Check important files exist
    important_files = [
        "requirements.txt",
        "Dockerfile", 
        "railway.json",
        "orchestrator/app.py",
        "launcher.py"
    ]
    
    for file_path in important_files:
        env_info["file_checks"][file_path] = os.path.exists(file_path)
    
    # Check package versions
    try:
        import flask
        env_info["package_versions"]["flask"] = flask.__version__
    except ImportError:
        env_info["package_versions"]["flask"] = "not_installed"
    
    try:
        import pandas
        env_info["package_versions"]["pandas"] = pandas.__version__
    except ImportError:
        env_info["package_versions"]["pandas"] = "not_installed"
        
    try:
        import numpy
        env_info["package_versions"]["numpy"] = numpy.__version__
    except ImportError:
        env_info["package_versions"]["numpy"] = "not_installed"
    
    try:
        import requests
        env_info["package_versions"]["requests"] = requests.__version__
    except ImportError:
        env_info["package_versions"]["requests"] = "not_installed"
    
    return env_info

def print_environment_report(env_info):
    """Print a formatted environment report"""
    
    print("=" * 60)
    print("ENVIRONMENT VERIFICATION REPORT")
    print("=" * 60)
    print(f"Timestamp: {env_info['timestamp']}")
    print()
    
    print("PLATFORM INFO:")
    print(f"  System: {env_info['platform']['system']} {env_info['platform']['release']}")
    print(f"  Machine: {env_info['platform']['machine']}")
    print()
    
    print("PYTHON INFO:")
    print(f"  Version: {env_info['python']['version'].split()[0]}")
    print(f"  Executable: {env_info['python']['executable']}")
    print()
    
    print("ENVIRONMENT VARIABLES:")
    for key, value in env_info['environment_vars'].items():
        print(f"  {key}: {value}")
    print()
    
    print("FILE CHECKS:")
    for file_path, exists in env_info['file_checks'].items():
        status = "✅ EXISTS" if exists else "❌ MISSING"
        print(f"  {file_path}: {status}")
    print()
    
    print("PACKAGE VERSIONS:")
    for package, version in env_info['package_versions'].items():
        print(f"  {package}: {version}")
    print()
    
    # Environment type detection
    is_railway = env_info['environment_vars']['RAILWAY_ENVIRONMENT'] != 'not_set'
    is_docker = os.path.exists('/.dockerenv')
    
    print("ENVIRONMENT TYPE:")
    if is_railway:
        print("  🚂 RAILWAY DEPLOYMENT")
    elif is_docker:
        print("  🐳 DOCKER CONTAINER (Local)")
    else:
        print("  💻 LOCAL DEVELOPMENT")
    print()
    
    print("CONSISTENCY CHECKS:")
    if env_info['package_versions']['flask'].startswith('2.3.3'):
        print("  ✅ Flask version matches requirements.txt")
    else:
        print("  ❌ Flask version mismatch - check requirements.txt")
        
    if env_info['python']['version'].startswith('3.9'):
        print("  ✅ Python version matches Dockerfile")
    else:
        print("  ❌ Python version mismatch - check Dockerfile")
    
    print("=" * 60)

def save_environment_snapshot(env_info, filename=None):
    """Save environment info to a JSON file for comparison"""
    
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        env_type = "railway" if env_info['environment_vars']['RAILWAY_ENVIRONMENT'] != 'not_set' else "local"
        filename = f"environment_snapshot_{env_type}_{timestamp}.json"
    
    with open(filename, 'w') as f:
        json.dump(env_info, f, indent=2)
    
    print(f"Environment snapshot saved to: {filename}")
    return filename

def compare_environments(local_file, railway_file):
    """Compare two environment snapshots"""
    
    try:
        with open(local_file) as f:
            local_env = json.load(f)
        with open(railway_file) as f:
            railway_env = json.load(f)
    except FileNotFoundError as e:
        print(f"Error: Could not find file {e.filename}")
        return
    
    print("ENVIRONMENT COMPARISON:")
    print("=" * 60)
    
    # Compare package versions
    print("PACKAGE VERSION COMPARISON:")
    local_packages = local_env.get('package_versions', {})
    railway_packages = railway_env.get('package_versions', {})
    
    all_packages = set(local_packages.keys()) | set(railway_packages.keys())
    
    for package in sorted(all_packages):
        local_ver = local_packages.get(package, 'missing')
        railway_ver = railway_packages.get(package, 'missing')
        
        if local_ver == railway_ver:
            print(f"  ✅ {package}: {local_ver}")
        else:
            print(f"  ❌ {package}: local={local_ver}, railway={railway_ver}")
    
    print("=" * 60)

if __name__ == "__main__":
    # Collect environment info
    env_info = get_environment_info()
    
    # Print report
    print_environment_report(env_info)
    
    # Save snapshot
    snapshot_file = save_environment_snapshot(env_info)
    
    print("\nTo compare with another environment:")
    print(f"1. Run this script in the other environment")
    print(f"2. Use: python verify-environment.py --compare local.json railway.json")
    
    # Handle comparison if files provided
    if len(sys.argv) > 3 and sys.argv[1] == "--compare":
        compare_environments(sys.argv[2], sys.argv[3]) 