#!/usr/bin/env python3
"""
Script to decompress all .gz files in the data directory to normal JSON files
"""
import gzip
import json
import os
from pathlib import Path

def decompress_gz_files(data_dir):
    """Decompress all .gz files in the data directory"""
    data_path = Path(data_dir)
    
    if not data_path.exists():
        print(f"❌ Data directory {data_dir} does not exist!")
        return
    
    # Find all .gz files
    gz_files = list(data_path.rglob("*.json.gz"))
    
    if not gz_files:
        print(f"❌ No .gz files found in {data_dir}")
        return
    
    print(f"🔍 Found {len(gz_files)} .gz files to decompress")
    
    success_count = 0
    for gz_file in gz_files:
        try:
            # Create output filename (remove .gz extension)
            json_file = gz_file.with_suffix('')  # Remove .gz, keep .json
            
            print(f"📥 Decompressing: {gz_file.name} -> {json_file.name}")
            
            # Decompress file
            with gzip.open(gz_file, 'rt', encoding='utf-8') as f_in:
                with open(json_file, 'w', encoding='utf-8') as f_out:
                    f_out.write(f_in.read())
            
            # Get file sizes for logging
            original_size = gz_file.stat().st_size
            decompressed_size = json_file.stat().st_size
            
            print(f"   ✓ {original_size:,} bytes -> {decompressed_size:,} bytes")
            success_count += 1
            
        except Exception as e:
            print(f"   ❌ Error decompressing {gz_file.name}: {e}")
    
    print(f"\n🎉 Decompression completed!")
    print(f"📊 Successfully decompressed: {success_count}/{len(gz_files)} files")
    
    # Show summary
    show_decompressed_summary(data_path)

def show_decompressed_summary(data_path):
    """Show summary of decompressed files"""
    json_files = list(data_path.rglob("*.json"))
    gz_files = list(data_path.rglob("*.json.gz"))
    
    total_json_size = sum(f.stat().st_size for f in json_files)
    total_gz_size = sum(f.stat().st_size for f in gz_files)
    
    print(f"\n📊 === FILE SUMMARY ===")
    print(f"📄 JSON files: {len(json_files)} ({total_json_size / (1024*1024):.1f} MB)")
    print(f"🗜️  GZ files: {len(gz_files)} ({total_gz_size / (1024*1024):.1f} MB)")
    
    if len(json_files) > 0:
        print(f"\n🔍 === HOW TO INSPECT JSON FILES ===")
        print(f"1. Event files are in: {data_path}/events/YYYY-MM-DD/")
        print(f"2. User files are in: {data_path}/users/")
        print(f"3. Files are now normal JSON - you can:")
        print(f"   - cat filename.json | head -10  (first 10 lines)")
        print(f"   - cat filename.json | jq '.' | head  (pretty-print)")
        print(f"   - grep 'RC Trial' filename.json  (search for specific events)")
        print(f"   - wc -l filename.json  (count events in file)")

def main():
    data_dir = "/Users/joshuakaufman/Atly Cursor Projects/Ads-Dashboard-Final/data"
    
    print("🗜️  === DECOMPRESS DATA FILES ===")
    print(f"Decompressing all .gz files in: {data_dir}")
    
    decompress_gz_files(data_dir)

if __name__ == "__main__":
    main() 