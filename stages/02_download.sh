#!/usr/bin/bash

set -euo pipefail

# Use the Python from Nix store that has all required packages including aiofiles
exec python3 stages/02_download_async.py