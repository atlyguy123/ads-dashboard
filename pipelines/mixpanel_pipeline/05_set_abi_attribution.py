#!/usr/bin/env python3
"""
Module 3: Set HasABI Attribution
Sets has_abi_attribution=TRUE for users who have abi_ad_id set (not null).
"""
import os
import sys
import sqlite3
from pathlib import Path

# Configuration - find database path robustly
script_dir = Path(__file__).parent  # pipelines/mixpanel_pipeline/
project_root = None

# Try different levels to find the project root that contains database/mixpanel_data.db
potential_roots = [
    script_dir.parent.parent,  # Go up from pipelines/mixpanel_pipeline/ to project root
    script_dir.parent.parent.parent,  # In case we're nested deeper
    Path.cwd().parent,  # Parent of current working directory
    Path.cwd().parent.parent,  # Grandparent of current working directory
]

for potential_root in potential_roots:
    db_path = potential_root / "database" / "mixpanel_data.db"
    if db_path.exists():
        project_root = potential_root
        break

if project_root is None:
    # Fallback: walk up from script location looking for database/mixpanel_data.db
    current = script_dir
    while current != current.parent:  # Stop at filesystem root
        db_path = current / "database" / "mixpanel_data.db"
        if db_path.exists():
            project_root = current
            break
        current = current.parent
    
    if project_root is None:
        raise FileNotFoundError("Could not locate project root directory containing 'database/mixpanel_data.db'")

DATABASE_PATH = project_root / "database" / "mixpanel_data.db"

def main():
    try:
        print("=== Module 5: Set HasABI Attribution ===")
        print("Starting ABI attribution analysis...")
        
        # Connect to database
        conn = sqlite3.connect(str(DATABASE_PATH))
        
        # Set ABI attribution flags
        set_abi_attribution_flags(conn)
        
        # Verify the results
        verify_abi_attribution(conn)
        
        conn.close()
        
        print("ABI attribution analysis completed successfully")
        return 0
        
    except Exception as e:
        print(f"Module 5 failed: {e}", file=sys.stderr)
        return 1

def set_abi_attribution_flags(conn):
    """Set has_abi_attribution based on abi_ad_id being not null"""
    print("Setting ABI attribution flags based on abi_ad_id...")
    
    cursor = conn.cursor()
    
    # Set has_abi_attribution = TRUE where abi_ad_id IS NOT NULL, FALSE otherwise
    cursor.execute("""
        UPDATE mixpanel_user 
        SET has_abi_attribution = (abi_ad_id IS NOT NULL)
    """)
    
    updated_rows = cursor.rowcount
    print(f"Updated {updated_rows} user records")
    
    conn.commit()

def verify_abi_attribution(conn):
    """Verify the ABI attribution results"""
    print("Verifying ABI attribution results...")
    
    cursor = conn.cursor()
    
    # Get overall statistics
    cursor.execute("SELECT COUNT(*) FROM mixpanel_user")
    total_users = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM mixpanel_user WHERE has_abi_attribution = TRUE")
    abi_users = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM mixpanel_user WHERE has_abi_attribution = FALSE")
    non_abi_users = cursor.fetchone()[0]
    
    # Calculate percentage
    abi_percentage = (abi_users / total_users * 100) if total_users > 0 else 0
    
    print(f"ABI Attribution Summary:")
    print(f"  - Total users: {total_users}")
    print(f"  - Users with ABI attribution: {abi_users} ({abi_percentage:.1f}%)")
    print(f"  - Users without ABI attribution: {non_abi_users}")
    
    # Sample some users with ABI attribution for verification
    cursor.execute("""
        SELECT distinct_id, abi_ad_id
        FROM mixpanel_user 
        WHERE has_abi_attribution = TRUE 
        LIMIT 3
    """)
    
    sample_users = cursor.fetchall()
    if sample_users:
        print(f"\nSample users with ABI attribution:")
        for distinct_id, abi_ad_id in sample_users:
            print(f"  - User {distinct_id}: abi_ad_id={abi_ad_id[:16]}...")
    
    # Check for any potential issues
    cursor.execute("""
        SELECT COUNT(*) 
        FROM mixpanel_user 
        WHERE has_abi_attribution IS NULL
    """)
    
    null_attribution = cursor.fetchone()[0]
    if null_attribution > 0:
        print(f"WARNING: {null_attribution} users have NULL attribution status")
    
    print("ABI attribution verification completed")

if __name__ == "__main__":
    sys.exit(main()) 