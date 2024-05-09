#!/usr/bin/env bash

set -eu

# Script to download files

# Get local path
localpath=$(pwd)
echo "Local path: $localpath"

# Set list path
listpath="$localpath"/data
echo "List path: $listpath"

# Create list path
mkdir -p "$listpath"
cd "$listpath"

# Define the endpoint base address
endpoint="https://api.fda.gov"

# Retrieve the list of files to download from endpoint
wget $endpoint/download.json

echo "Download done."
