#!/usr/bin/env bash

set -eu

source stages/00_config.sh

# Create brick directory
mkdir -p "$OPENFDA_BRICK_PATH"

echo "OpenFDA Build Script"
echo "==================="
echo "Raw path: $OPENFDA_RAW_PATH"
echo "Brick path: $OPENFDA_BRICK_PATH"
echo "Processing files from download manifest..."

# Call the Python build script
python3 stages/build_parquet.py

echo "Build process completed."