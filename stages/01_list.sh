#!/usr/bin/env bash

set -eu

# Script to download files

# Get local path
localpath=$(pwd)
echo "Local path: $localpath"

# Set list path
listpath="$localpath"/list
echo "List path: $listpath"

# Create list path
mkdir -p "$listpath"
cd "$listpath"

# Define the endpoint base address
endpoint="https://api.fda.gov"

# Retrieve the list of files to download from endpoint
wget $endpoint/download.json

cp download.json download_full.json
jq '{
    meta: .meta,
    results: {
    drug: {
      drugsfda: .results.drug.drugsfda,
      label: .results.drug.label,
      ndc: .results.drug.ndc
    },
    other: {
      nsde: .results.other.nsde,
      unii: .results.other.unii,
      substance: .results.other.substance
    }
  }
}' download_full.json > download.json
rm download_full.json

echo "Download done."
