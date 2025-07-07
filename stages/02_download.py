#!/usr/bin/env python3

import json
import os
import sys
import tempfile
import shutil
from pathlib import Path
from multiprocessing import Pool, cpu_count
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
import requests
from datetime import datetime
import time
from tqdm import tqdm
import threading

# Thread-local storage for sessions
_thread_local = threading.local()

def get_session():
    """Get or create a thread-local requests session"""
    if not hasattr(_thread_local, 'session'):
        _thread_local.session = requests.Session()
        _thread_local.session.headers.update({
            'User-Agent': 'OpenFDA-Downloader/1.0'
        })
        # Enable keep-alive and connection pooling
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20,
            pool_maxsize=50,
            max_retries=3
        )
        _thread_local.session.mount('http://', adapter)
        _thread_local.session.mount('https://', adapter)
    return _thread_local.session

def parse_date(date_string):
    """Parse date string to timestamp for comparison"""
    try:
        # Handle various date formats that might be encountered
        formats = [
            '%a, %d %b %Y %H:%M:%S %Z',  # HTTP date format
            '%Y-%m-%d',                   # Simple date format
            '%Y-%m-%dT%H:%M:%S',         # ISO format without timezone
            '%Y-%m-%dT%H:%M:%SZ',        # ISO format with Z
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_string.strip(), fmt).timestamp()
            except ValueError:
                continue
        
        # If no format matches, try a more flexible approach
        import dateutil.parser
        return dateutil.parser.parse(date_string).timestamp()
    except Exception:
        # If all parsing fails, return 0 (epoch) to force download
        return 0

def download_single_file(args):
    """Download a single file with conditional logic"""
    data_type, field_name, partition_index, listjson_path = args
    
    try:
        # Load and parse JSON data
        with open(listjson_path, 'r') as f:
            data = json.load(f)
        
        # Extract file information
        file_info = data['results'][data_type][field_name]
        export_date = file_info['export_date']
        file_url = file_info['partitions'][partition_index]['file']
        
        # Parse file paths
        filename = os.path.basename(file_url)
        dir_name = os.path.basename(os.path.dirname(file_url))
        download_dir = Path('.') / data_type / dir_name
        final_file = download_dir / filename
        last_modified_file = download_dir / f"{filename}.last-modified"
        
        # Create directory if it doesn't exist
        download_dir.mkdir(parents=True, exist_ok=True)
        
        # Check if file exists and get previous last-modified date
        if last_modified_file.exists() and final_file.exists():
            with open(last_modified_file, 'r') as f:
                previous_last_modified = f.read().strip()
            
            # Compare dates - if previous is newer than export date, skip
            if parse_date(previous_last_modified) > parse_date(export_date):
                return f"Skipped {filename} (up to date)"
        else:
            previous_last_modified = None
        
        # Get thread-local session
        session = get_session()
        
        # Prepare headers for conditional download
        headers = {}
        if previous_last_modified:
            headers['If-Modified-Since'] = previous_last_modified
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False, dir=tempfile.gettempdir()) as tmp_file:
            tmp_filename = tmp_file.name
        
        try:
            # Make the request
            response = session.get(
                file_url, 
                headers=headers, 
                timeout=300,
                stream=True  # Stream to handle large files efficiently
            )
            
            # Handle 304 Not Modified
            if response.status_code == 304:
                os.unlink(tmp_filename)
                return f"Skipped {filename} (not modified)"
            
            # Check for successful response
            response.raise_for_status()
            
            # Write file content
            with open(tmp_filename, 'wb') as tmp_file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # Filter out keep-alive chunks
                        tmp_file.write(chunk)
            
            # Atomically move file to final destination
            shutil.move(tmp_filename, final_file)
            
            # Save Last-Modified header if present
            last_modified = response.headers.get('Last-Modified')
            if last_modified:
                with open(last_modified_file, 'w') as f:
                    f.write(last_modified)
            
            return f"Downloaded {filename} ({len(response.content) if hasattr(response, 'content') else 'streamed'} bytes)"
            
        except requests.exceptions.RequestException as e:
            # Clean up temporary file on error
            if os.path.exists(tmp_filename):
                os.unlink(tmp_filename)
            return f"Failed {filename}: {str(e)}"
        
        finally:
            pass  # Don't close thread-local session
            
    except Exception as e:
        return f"Error processing {filename}: {str(e)}"

def download_with_threads(download_list, num_files):
    """Download using ThreadPoolExecutor for better I/O concurrency"""
    # Use more threads since downloads are I/O-bound
    max_workers = min(20, num_files)
    print(f"Using {max_workers} worker threads")
    
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        with tqdm(total=num_files, desc="Downloading", unit="file") as pbar:
            # Submit all tasks
            future_to_args = {executor.submit(download_single_file, args): args for args in download_list}
            
            # Process completed tasks
            for future in as_completed(future_to_args):
                result = future.result()
                results.append(result)
                pbar.set_postfix_str(result.split()[1] if len(result.split()) > 1 else "")
                pbar.update(1)
    
    return results

def download_with_processes(download_list, num_files):
    """Download using multiprocessing (original method)"""
    # Use fewer processes than CPU cores to avoid overwhelming the server
    num_workers = min(cpu_count(), 8, num_files)
    print(f"Using {num_workers} worker processes")
    
    results = []
    with Pool(processes=num_workers) as pool:
        with tqdm(total=num_files, desc="Downloading", unit="file") as pbar:
            for result in pool.imap_unordered(download_single_file, download_list):
                results.append(result)
                pbar.set_postfix_str(result.split()[1] if len(result.split()) > 1 else "")
                pbar.update(1)
    
    return results

def main():
    """Main download function"""
    print("Python OpenFDA Downloader (Enhanced)")
    print("=" * 40)
    
    # Setup paths
    local_path = Path.cwd()
    list_path = local_path / "list"
    listjson_path = list_path / "download.json"
    download_path = local_path / "raw"
    
    print(f"Local path: {local_path}")
    print(f"List path: {list_path}")
    print(f"Download path: {download_path}")
    
    # Create download directory
    download_path.mkdir(exist_ok=True)
    
    # Change to download directory for relative path operations
    os.chdir(download_path)
    
    # Load download manifest
    try:
        with open(listjson_path, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: {listjson_path} not found")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        sys.exit(1)
    
    # Generate download list
    download_list = []
    
    for data_type in data['results']:
        for field_name in data['results'][data_type]:
            partitions = data['results'][data_type][field_name]['partitions']
            for partition_index in range(len(partitions)):
                download_list.append((data_type, field_name, partition_index, str(listjson_path)))
    
    num_files = len(download_list)
    print(f"Found {num_files} files to download")
    
    if num_files == 0:
        print("No files to download")
        return
    
    # Choose concurrency method based on environment variable or default to threads
    concurrency_method = os.environ.get('DOWNLOAD_METHOD', 'threads').lower()
    
    print("Starting parallel downloads...")
    
    if concurrency_method == 'processes':
        results = download_with_processes(download_list, num_files)
    else:  # Default to threads for better I/O concurrency
        results = download_with_threads(download_list, num_files)
    
    print("\nDownload Summary:")
    print("-" * 40)
    
    # Categorize results
    downloaded = [r for r in results if r.startswith("Downloaded")]
    skipped = [r for r in results if r.startswith("Skipped")]
    failed = [r for r in results if r.startswith("Failed") or r.startswith("Error")]
    
    print(f"Downloaded: {len(downloaded)}")
    print(f"Skipped: {len(skipped)}")
    print(f"Failed: {len(failed)}")
    
    if failed:
        print("\nFailed downloads:")
        for failure in failed[:5]:  # Show first 5 failures
            print(f"  {failure}")
        if len(failed) > 5:
            print(f"  ... and {len(failed) - 5} more")
    
    print("Download completed!")
    
    # Exit with error code if there were failures
    if failed:
        sys.exit(1)

if __name__ == "__main__":
    main() 