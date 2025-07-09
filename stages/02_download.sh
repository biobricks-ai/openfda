#!/usr/bin/env bash

set -euo pipefail

# Script to download files using GNU parallel

# Check if GNU parallel is available
if ! command -v parallel &> /dev/null; then
    echo "Error: GNU parallel is not installed. Please install it first." >&2
    echo "On Ubuntu/Debian: sudo apt-get install parallel" >&2
    echo "On CentOS/RHEL: sudo yum install parallel" >&2
    echo "On macOS: brew install parallel" >&2
    exit 1
fi

# Get local path
localpath=$(pwd)
echo "Local path: $localpath"

# Set list path
listpath="$localpath/data"
echo "List path: $listpath"
listjson="$(realpath "$listpath/download.json")"

# Set download path
downloadpath="$localpath/download"
echo "Download path: $downloadpath"
mkdir -p "$downloadpath"
cd "$downloadpath"

# Download a single partition file
download_partition() {
  local data_type=$1
  local field_name=$2
  local partition_index=$3
  local listjson=$4

  local export_date
  local file_url
  local filename
  local dir
  local last_modified_file
  local previous_last_modified_date
  local headers
  local status
  local last_modified

  export_date=$(jq -r ".results.\"${data_type}\".\"${field_name}\".export_date" "$listjson")
  file_url=$(jq -r ".results.\"${data_type}\".\"${field_name}\".partitions[$partition_index].file" "$listjson")
  filename=$(basename "$file_url")
  dir="${data_type}"/$(basename "$(dirname "$file_url")")
  # git doesn't preserve mtimes, so we record them separately
  last_modified_file="./$dir/${filename}.last-modified"
  mkdir -p "$dir"

  

  # Check if there's a previous Last-Modified date saved
  if [[ -f "$last_modified_file" ]]; then
      previous_last_modified_date=$(cat "$last_modified_file")

      if [[ $(date -d "$previous_last_modified_date" +%s) \
            -gt $(date -d "$export_date" +%s) ]]; then
          return 0
      fi

      touch --no-create -t "$(date -d "$previous_last_modified_date" +'%Y%m%d%H%M.%S')" ./"$dir"/"$filename"
      headers=$(wget -S --timeout=300 --tries=3 -N --header "If-Modified-Since: $previous_last_modified_date" -P ./"$dir"/ "$file_url" 2>&1 || true)
      status=$?

      # Don't exit on 304 not modified responses, but exit on other errors
      if [[ $status -eq 8 ]] && ! echo "$headers" | grep -qi "HTTP/1.1 304"; then
          echo "$headers" >&2
          return 1
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
}

# Export the function so parallel can use it
export -f download_partition

# Create a list of all download tasks (data_type field_name partition_index)
echo "Generating download task list..."
download_tasks_file=$(mktemp)

# Get the list of data types (e.g., food, animalandveterinary)
data_types=$(jq -r '.results | keys[]' "$listjson")

# Generate all download tasks
for data_type in $data_types; do
  # Get the list of field names (e.g., enforcement, event, 510k) for each data type
  field_names=$(jq -r ".results.\"${data_type}\" | keys[]" "$listjson")
  
  for field_name in $field_names; do
    # Get the number of partitions
    num_partitions=$(jq -r ".results.\"${data_type}\".\"${field_name}\".partitions | length" "$listjson")
    
    # Add each partition as a separate task
    for (( i=0; i<num_partitions; i++ )); do
      echo "$data_type $field_name $i $listjson" >> "$download_tasks_file"
    done
  done
done

total_tasks=$(wc -l < "$download_tasks_file")
echo "Total download tasks: $total_tasks"

# Run downloads in parallel
# Limit to 4 concurrent downloads to avoid overwhelming the server
echo "Starting parallel downloads (max 4 concurrent)..."
parallel --bar -j 4 --colsep ' ' download_partition {1} {2} {3} {4} :::: "$download_tasks_file"

# Clean up temporary file
rm "$download_tasks_file"

echo "All downloads completed!"
