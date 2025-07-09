import shutil
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

data_path = Path("download")
raw_path = Path("raw")

if not raw_path.exists():
    raw_path.mkdir(exist_ok=True)
    print(f"created {raw_path}")


def unpack(zipfile: Path):
    extract_to = raw_path / zipfile.relative_to(data_path).stem[:-5]
    if not extract_to.exists():
        print(f"extracting {zipfile} to {extract_to}")
        shutil.unpack_archive(zipfile, extract_to)
    else:
        print(f"skipping {zipfile} because {extract_to} already exists")
    if extract_to.exists() and os.stat(extract_to).st_size != 0:
        return extract_to
    return False


def exec(max_threads=4, directories=None):
    """
    Extract zip files from specified directories.
    
    Args:
        max_threads (int): Maximum number of threads for parallel processing
        directories (list): List of directory paths relative to data_path to extract from.
                          If None, extracts from all directories.
                          Example: ["drug/label", "drug/ndc", "other/substance"]
    """
    if directories is None:
        # Extract from all directories (original behavior)
        zipfiles = data_path.rglob("*.json.zip")
    else:
        # Extract only from specified directories
        zipfiles = []
        for directory in directories:
            dir_path = data_path / directory
            if dir_path.exists():
                zipfiles.extend(dir_path.glob("*.json.zip"))
                print(f"Found {len(list(dir_path.glob('*.json.zip')))} zip files in {directory}")
            else:
                print(f"Warning: Directory {directory} does not exist")
    
    with ThreadPoolExecutor(max_threads) as exec:
        futures = {
            exec.submit(unpack, zipfile): zipfile.name
            for zipfile in zipfiles
        }
        data = list(filter(None, (fut.result() for fut in as_completed(futures))))
        for name in data:
            print(f"conversion completed for {name}")


if __name__ == "__main__":
    # Example: Only extract from specific directories
    # Uncomment and modify the directories list as needed
    specified_dirs = [
        "drug/drugsfda",
        "drug/label", 
        "drug/ndc",
        "other/nsde",
        "other/substance",
        "other/unii"
    ]
    
    # To extract from all directories (original behavior), use:
    # exec()
    
    # To extract from specified directories only, use:
    exec(directories=specified_dirs)
