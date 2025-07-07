#!/usr/bin/env python3

import asyncio
import aiofiles
import aiohttp
import json
import os
import sys
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
import time
from tqdm import tqdm

async def parse_date(date_string):
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
        try:
            import dateutil.parser
            return dateutil.parser.parse(date_string).timestamp()
        except:
            pass
    except Exception:
        pass
    
    # If all parsing fails, return 0 (epoch) to force download
    return 0

async def download_single_file(session, semaphore, args, pbar):
    """Download a single file with async concurrency control"""
    data_type, field_name, partition_index, listjson_path, download_data = args
    
    async with semaphore:  # Limit concurrent downloads
        try:
            # Extract file information from pre-loaded data
            file_info = download_data['results'][data_type][field_name]
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
            previous_last_modified = None
            if last_modified_file.exists() and final_file.exists():
                async with aiofiles.open(last_modified_file, 'r') as f:
                    previous_last_modified = (await f.read()).strip()
                
                # Compare dates - if previous is newer than export date, skip
                if await parse_date(previous_last_modified) > await parse_date(export_date):
                    pbar.update(1)
                    return f"Skipped {filename} (up to date)"
            
            # Prepare headers for conditional download
            headers = {}
            if previous_last_modified:
                headers['If-Modified-Since'] = previous_last_modified
            
            # Create temporary file
            fd, tmp_filename = tempfile.mkstemp(dir=tempfile.gettempdir())
            os.close(fd)  # Close the file descriptor, we'll use aiofiles
            
            try:
                # Make the async request
                async with session.get(file_url, headers=headers) as response:
                    # Handle 304 Not Modified
                    if response.status == 304:
                        os.unlink(tmp_filename)
                        pbar.update(1)
                        return f"Skipped {filename} (not modified)"
                    
                    # Check for successful response
                    response.raise_for_status()
                    
                    # Write file content asynchronously
                    async with aiofiles.open(tmp_filename, 'wb') as tmp_file:
                        async for chunk in response.content.iter_chunked(8192):
                            await tmp_file.write(chunk)
                    
                    # Atomically move file to final destination
                    shutil.move(tmp_filename, final_file)
                    
                    # Save Last-Modified header if present
                    last_modified = response.headers.get('Last-Modified')
                    if last_modified:
                        async with aiofiles.open(last_modified_file, 'w') as f:
                            await f.write(last_modified)
                    
                    # Get file size for reporting
                    file_size = final_file.stat().st_size if final_file.exists() else 0
                    
                    pbar.update(1)
                    return f"Downloaded {filename} ({file_size} bytes)"
                    
            except aiohttp.ClientError as e:
                # Clean up temporary file on error
                if os.path.exists(tmp_filename):
                    os.unlink(tmp_filename)
                pbar.update(1)
                return f"Failed {filename}: {str(e)}"
            except Exception as e:
                # Clean up temporary file on error
                if os.path.exists(tmp_filename):
                    os.unlink(tmp_filename)
                pbar.update(1)
                return f"Error {filename}: {str(e)}"
                
        except Exception as e:
            pbar.update(1)
            return f"Error processing {filename}: {str(e)}"

async def main():
    """Main async download function"""
    print("Python OpenFDA Async Downloader")
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
        async with aiofiles.open(listjson_path, 'r') as f:
            content = await f.read()
            data = json.loads(content)
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
                download_list.append((data_type, field_name, partition_index, str(listjson_path), data))
    
    num_files = len(download_list)
    print(f"Found {num_files} files to download")
    
    if num_files == 0:
        print("No files to download")
        return
    
    # Configure concurrency settings
    # Use conservative concurrency to be respectful of FDA servers
    max_concurrent = min(10, num_files)  # Up to 10 concurrent downloads
    print(f"Using up to {max_concurrent} concurrent downloads")
    
    # Create semaphore to limit concurrent downloads
    semaphore = asyncio.Semaphore(max_concurrent)
    
    # Configure aiohttp session with conservative settings
    timeout = aiohttp.ClientTimeout(total=600, connect=60)  # Longer timeouts
    connector = aiohttp.TCPConnector(
        limit=20,  # Total connection pool size
        limit_per_host=3,  # Max connections per host - very conservative
        keepalive_timeout=60,
        enable_cleanup_closed=True
    )
    
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers={'User-Agent': 'OpenFDA-AsyncDownloader/1.0'}
    ) as session:
        
        print("Starting async downloads...")
        
        # Create progress bar
        with tqdm(total=num_files, desc="Downloading", unit="file") as pbar:
            # Create tasks for all downloads
            tasks = [
                download_single_file(session, semaphore, args, pbar)
                for args in download_list
            ]
            
            # Execute all downloads concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)
    
    print("\nDownload Summary:")
    print("-" * 40)
    
    # Process results and handle any exceptions
    processed_results = []
    for result in results:
        if isinstance(result, Exception):
            processed_results.append(f"Error: {str(result)}")
        else:
            processed_results.append(result)
    
    # Categorize results
    downloaded = [r for r in processed_results if r.startswith("Downloaded")]
    skipped = [r for r in processed_results if r.startswith("Skipped")]
    failed = [r for r in processed_results if r.startswith("Failed") or r.startswith("Error")]
    
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

def run_async_main():
    """Wrapper to run the async main function"""
    try:
        # Python 3.7+ way to run async code
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nDownload interrupted by user")
        sys.exit(130)

if __name__ == "__main__":
    run_async_main() 