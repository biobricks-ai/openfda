import shutil
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

data_path = Path("download")
raw_path = Path("raw")

if not raw_path.exists():
    raw_path.mkdir(exist_ok=True)


def unpack(zipfile):
    extract_to = raw_path / zipfile.relative_to(data_path).stem
    if not extract_to.exists():
        shutil.unpack_archive(zipfile, extract_to)
    if extract_to.exists() and os.stat(extract_to).st_size != 0:
        return extract_to
    return False


def exec(max_threads=4):
    with ThreadPoolExecutor(max_threads) as exec:
        futures = {
            exec.submit(unpack, zipfile): zipfile.name
            for zipfile in data_path.rglob("*.json.zip")
        }
        data = list(filter(None, (fut.result() for fut in as_completed(futures))))
        for name in data:
            print(f"conversion completed for {name}")


if __name__ == "__main__":
    exec()
