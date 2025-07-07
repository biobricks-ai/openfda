#!/usr/bin/env bash

set -eu

source stages/00_config.sh

# Script to create list of files to download

# Create list path
mkdir -p "$OPENFDA_LIST_PATH"

# Retrieve the list of files to download from endpoint
wget "$OPENFDA_ENDPOINT/download.json" -O "$OPENFDA_LIST_PATH/download.json"

echo "Download done."
