#!/usr/bin/env python3

import json
import os
import sys
import tempfile
import shutil
import zipfile
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

def process_single_file(args):
    """Process a single file: unzip if needed, convert to parquet"""
    data_type, field_name, partition_index, download_data = args
    
    try:
        # Extract file information
        file_info = download_data['results'][data_type][field_name]
        file_url = file_info['partitions'][partition_index]['file']
        
        # Parse file paths (same logic as download script)
        filename = os.path.basename(file_url)
        dir_name = os.path.basename(os.path.dirname(file_url))
        
        # Input paths in raw directory
        raw_dir = Path(os.environ['OPENFDA_RAW_PATH']) / data_type / dir_name
        raw_file = raw_dir / filename
        
        # Output paths in brick directory (maintain same structure)
        brick_dir = Path(os.environ['OPENFDA_BRICK_PATH']) / data_type / dir_name
        brick_dir.mkdir(parents=True, exist_ok=True)
        
        # Determine output filename (remove .zip, change .json to .parquet)
        if filename.endswith('.json.zip'):
            base_name = filename[:-9]  # Remove .json.zip
        elif filename.endswith('.zip'):
            base_name = filename[:-4]   # Remove .zip
        else:
            base_name = filename
        
        brick_file = brick_dir / f"{base_name}.parquet"
        
        # Skip if output already exists and is newer than input
        if brick_file.exists() and raw_file.exists():
            if brick_file.stat().st_mtime > raw_file.stat().st_mtime:
                return f"Skipped {data_type}/{dir_name}/{filename} (up to date)"
        
        # Check if input file exists
        if not raw_file.exists():
            return f"Missing {data_type}/{dir_name}/{filename}"
        
        # Create temporary directory for extraction
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Handle zip files
            if filename.endswith('.zip'):
                try:
                    with zipfile.ZipFile(raw_file, 'r') as zip_ref:
                        zip_ref.extractall(temp_path)
                    
                    # Find the extracted JSON file
                    json_files = list(temp_path.glob('*.json'))
                    if not json_files:
                        return f"Error: No JSON file found in {filename}"
                    
                    json_file = json_files[0]  # Use the first JSON file found
                    
                except zipfile.BadZipFile:
                    return f"Error: Bad zip file {filename}"
            else:
                # If not a zip file, use the file directly
                json_file = raw_file
            
            # Convert JSON to Parquet using the existing script
            result = os.system(f"python3 stages/json2parquet.py '{json_file}' '{brick_file}'")
            
            if result == 0:
                file_size = brick_file.stat().st_size if brick_file.exists() else 0
                return f"Converted {data_type}/{dir_name}/{filename} ({file_size} bytes)"
            else:
                return f"Failed {data_type}/{dir_name}/{filename}"
                
    except Exception as e:
        return f"Error processing {data_type}/{dir_name}/{filename}: {str(e)}"

def main():
    """Main build function"""
    print("OpenFDA Parquet Builder")
    print("=" * 40)
    
    # Load download manifest
    list_path = Path(os.environ['OPENFDA_LIST_PATH'])
    listjson_path = list_path / "download.json"
    
    try:
        with open(listjson_path, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: {listjson_path} not found")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        sys.exit(1)
    
    # Generate processing list
    process_list = []
    
    for data_type in data['results']:
        for field_name in data['results'][data_type]:
            partitions = data['results'][data_type][field_name]['partitions']
            for partition_index in range(len(partitions)):
                process_list.append((data_type, field_name, partition_index, data))
    
    num_files = len(process_list)
    print(f"Found {num_files} files to process")
    
    if num_files == 0:
        print("No files to process")
        return
    
    # Process files in parallel
    max_workers = min(14, num_files)  # Match the original parallelism
    print(f"Using {max_workers} worker threads")
    
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        with tqdm(total=num_files, desc="Processing", unit="file") as pbar:
            # Submit all tasks
            future_to_args = {executor.submit(process_single_file, args): args for args in process_list}
            
            # Process completed tasks
            for future in as_completed(future_to_args):
                result = future.result()
                results.append(result)
                pbar.set_postfix_str(result.split()[1] if len(result.split()) > 1 else "")
                pbar.update(1)
    
    print("\nBuild Summary:")
    print("-" * 40)
    
    # Categorize results
    converted = [r for r in results if r.startswith("Converted")]
    skipped = [r for r in results if r.startswith("Skipped")]
    missing = [r for r in results if r.startswith("Missing")]
    failed = [r for r in results if r.startswith("Failed") or r.startswith("Error")]
    
    print(f"Converted: {len(converted)}")
    print(f"Skipped: {len(skipped)}")
    print(f"Missing: {len(missing)}")
    print(f"Failed: {len(failed)}")
    
    if missing:
        print("\nMissing files (may need to download first):")
        for miss in missing[:5]:  # Show first 5 missing
            print(f"  {miss}")
        if len(missing) > 5:
            print(f"  ... and {len(missing) - 5} more")
    
    if failed:
        print("\nFailed conversions:")
        for failure in failed[:5]:  # Show first 5 failures
            print(f"  {failure}")
        if len(failed) > 5:
            print(f"  ... and {len(failed) - 5} more")
    
    print("Build completed!")
    
    # Exit with error code if there were failures
    if failed:
        sys.exit(1)

if __name__ == "__main__":
    main() 