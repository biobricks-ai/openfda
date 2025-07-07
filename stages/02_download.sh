#!/usr/bin/env bash

set -euo pipefail

# Use the Python from Nix store that has all required packages including aiofiles
exec /nix/store/539v4sbanp0crxpk06y0236mqaa8k2z8-python3-3.13.4-env/bin/python3 stages/02_download_async.py