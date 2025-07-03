#!/usr/bin/env bash

set -euo pipefail

# Fix locale warnings
export LC_ALL=C
export LANG=C

# Script to download files in parallel

# Get local path
localpath=$(pwd)
echo "Local path: $localpath"

# Set list path
listpath="$localpath/list"
echo "List path: $listpath"
listjson="$(realpath "$listpath/download.json")"

# Set download path
downloadpath="$localpath/raw"
echo "Download path: $downloadpath"
mkdir -p "$downloadpath"
cd "$downloadpath"

# Download a single file
download_single_file() {
  {
    # Parse the input line
    IFS=$'\t' read -r data_type field_name partition_index listjson_path <<< "$1"

    local last_modified
    export_date=$(jq -r ".results.\"${data_type}\".\"${field_name}\".export_date" "$listjson_path")
    file_url=$(jq -r ".results.\"${data_type}\".\"${field_name}\".partitions[$partition_index].file" "$listjson_path")
    filename=$(basename "$file_url")
    dir="${data_type}"/$(basename "$(dirname "$file_url")")
    final_file="./$dir/$filename"
    # git doesn't preserve mtimes, so we record them separately
    last_modified_file="./$dir/${filename}.last-modified"
    mkdir -p "$dir"

    # Check if there's a previous Last-Modified date saved and file exists
    if [[ -f "$last_modified_file" && -f "$final_file" ]]; then
        previous_last_modified_date=$(cat "$last_modified_file")

        if [[ $(date -d "$previous_last_modified_date" +%s) \
              -gt $(date -d "$export_date" +%s) ]]; then
            return 0
        fi

        # Create temporary directory for this download
        tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/openfda_download.XXXXXX")
        tmp_file="$tmp_dir/$filename"
        
        # Use conditional download with If-Modified-Since
        headers=$(wget -q -S --timeout=300 --tries=3 --header "If-Modified-Since: $previous_last_modified_date" -O "$tmp_file" "$file_url" 2>&1 || true)
        status=$?

        # Check if file was not modified (304 response)
        if [[ $status -eq 8 ]] && echo "$headers" | grep -qi "HTTP/1.1 304" >/dev/null; then
            # File not modified, clean up and return
            rm -rf "$tmp_dir"
            return 0
        elif [[ $status -ne 0 ]]; then
            # Download failed
            rm -rf "$tmp_dir"
            return 1
        fi
    else
        # No previous download or file missing, download fresh
        # Create temporary directory for this download
        tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/openfda_download.XXXXXX")
        tmp_file="$tmp_dir/$filename"
        
        headers=$(wget -q -S --timeout=300 --tries=3 -O "$tmp_file" "$file_url" 2>&1)
        status=$?
        
        if [[ $status -ne 0 ]]; then
            # Download failed
            rm -rf "$tmp_dir"
            return 1
        fi
    fi

    # Download successful, atomically move file to final destination
    mv "$tmp_file" "$final_file"
    
    # Extract and save Last-Modified header
    {
      last_modified=$(echo "$headers" | grep -i "Last-Modified" | awk -F': ' '{print $2}')
    } || true
    if [[ $last_modified ]]; then
        echo "$last_modified" > "$last_modified_file"
    fi
    
    # Clean up temporary directory
    rm -rf "$tmp_dir"
  } >/dev/null 2>/dev/null
}

# Export the function so parallel can use it
export -f download_single_file

# Generate list of all files to download as tab-separated values
download_list_file=$(mktemp)

# Get the list of data types (e.g., food, animalandveterinary)
data_types=$(jq -r '.results | keys[]' "$listjson")

# Iterate through each data type
for data_type in $data_types; do
  # Get the list of field names (e.g., enforcement, event, 510k) for each data type
  field_names=$(jq -r ".results.\"${data_type}\" | keys[]" "$listjson")

  # Iterate through each field name
  for field_name in $field_names; do
    # Get the number of partitions
    num_partitions=$(jq -r ".results.\"${data_type}\".\"${field_name}\".partitions | length" "$listjson")
    
    # Add each partition to the download list as tab-separated values
    for (( i=0; i<num_partitions; i++ )); do
      echo -e "${data_type}\t${field_name}\t${i}\t${listjson}" >> "$download_list_file"
    done
  done
done

num_files=$(wc -l < "$download_list_file")
echo "Found $num_files files to download"

# Use GNU parallel to download files in parallel with --bar progress indicator
if [[ $num_files -gt 0 ]]; then
  echo "Starting parallel downloads..."
  cat "$download_list_file" | parallel --no-notice --bar --line-buffer download_single_file {}
  echo "Download completed!"
else
  echo "No files to download"
fi

# Clean up temporary file
rm -f "$download_list_file"
