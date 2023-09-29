#!/usr/bin/env bash

set -euo pipefail

# Script to unzip files

# Get local path
localpath=$(pwd)
echo "Local path: $localpath"

# Set list path
listpath="$localpath/list"
echo "List path: $listpath"
listjson="$(realpath "$listpath/download.json")"

# Set download path
downloadpath="$localpath/download"
echo "Download path: $downloadpath"
mkdir -p "$downloadpath"
cd "$downloadpath"

# Download zipped JSON data files
download_files() {
  local data_type=$1
  local field_name=$2

  # Get the number of partitions
  local num_partitions
  num_partitions=$(jq -r ".results.\"${data_type}\".\"${field_name}\".partitions | length" "$listjson")

  # Iterate through each partition and download the file
  for (( i=0; i<num_partitions; i++ )); do
    local last_modified
    export_date=$(jq -r ".results.\"${data_type}\".\"${field_name}\".export_date" "$listjson")
    file_url=$(jq -r ".results.\"${data_type}\".\"${field_name}\".partitions[$i].file" "$listjson")
    filename=$(basename "$file_url")
    dir="${data_type}"/$(basename "$(dirname "$file_url")")
    # git doesn't preserve mtimes, so we record them separately
    last_modified_file="./$dir/${filename}.last-modified"
    mkdir -p "$dir"

    echo "Processing ./$dir/$filename"

    # Check if there's a previous Last-Modified date saved
    if [[ -f "$last_modified_file" ]]; then
        previous_last_modified_date=$(cat "$last_modified_file")

        if [[ $(date -d "$previous_last_modified_date" +%s) \
              -gt $(date -d "$export_date" +%s) ]]; then
            echo "$dir"/"$filename" is already up-to-date
            continue
        fi

        touch --no-create -t "$(date -d "$previous_last_modified_date" +'%Y%m%d%H%M.%S')" ./"$dir"/"$filename"
        headers=$(wget -S --timeout=300 --tries=3 -N --header "If-Modified-Since: $previous_last_modified_date" -P ./"$dir"/ "$file_url" 2>&1 || true)
        status=$?

        # Don't exit on 304 not modified responses, but exit on other errors
        if [[ $status -eq 8 ]] && ! echo "$headers" | grep -qi "HTTP/1.1 304"; then
            echo "$headers" >&2
            exit 1
        fi
    else
        previous_last_modified_date=""
        headers=$(wget -S --timeout=300 --tries=3 -P ./"$dir"/ "$file_url" 2>&1)
    fi

    # Print results
    echo "$headers" | grep -i "saved" || true
    echo "$headers" | grep -i "not modified on server" || true

    # Use a command group to avoid errors when Last-Modified is not present.
    # This can occur when the existing file was already up-to-date.
    {
      last_modified=$(echo "$headers" | grep -i "Last-Modified" | awk -F': ' '{print $2}')
    } || true
    if [[ $last_modified ]]; then
        echo "$last_modified" > "$last_modified_file"
    fi
  done
}

# Get the list of data types (e.g., food, animalandveterinary)
data_types=$(jq -r '.results | keys[]' "$listjson")

# Iterate through each data type
for data_type in $data_types; do
  # Get the list of field names (e.g., enforcement, event, 510k) for each data type
  field_names=$(jq -r ".results.\"${data_type}\" | keys[]" "$listjson")

  # Iterate through each field name and call the download function
  for field_name in $field_names; do
    download_files "$data_type" "$field_name"
  done
done
