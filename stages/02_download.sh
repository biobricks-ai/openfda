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
downloadpath="$localpath/download"
echo "Download path: $downloadpath"
mkdir -p "$downloadpath"
cd "$downloadpath"

# Download a single file
download_single_file() {
  # Parse the input line
  IFS=$'\t' read -r data_type field_name partition_index listjson_path <<< "$1"

  local last_modified
  export_date=$(jq -r ".results.\"${data_type}\".\"${field_name}\".export_date" "$listjson_path")
  file_url=$(jq -r ".results.\"${data_type}\".\"${field_name}\".partitions[$partition_index].file" "$listjson_path")
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
      headers=$(wget -q -S --timeout=300 --tries=3 -N --header "If-Modified-Since: $previous_last_modified_date" -P ./"$dir"/ "$file_url" 2>&1 || true)
      status=$?

      # Don't exit on 304 not modified responses, but exit on other errors
      if [[ $status -eq 8 ]] && ! echo "$headers" | grep -qi "HTTP/1.1 304"; then
          echo "$headers" >&2
          return 1
      fi
  else
      previous_last_modified_date=""
      headers=$(wget -q -S --timeout=300 --tries=3 -P ./"$dir"/ "$file_url" 2>&1)
  fi

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

# Use GNU parallel to download files in parallel with a single clean progress bar
if [[ $num_files -gt 0 ]]; then
  echo "Starting parallel downloads..."
  
  # Create a custom progress function
  progress_file=$(mktemp)
  echo "0" > "$progress_file"
  
  # Modified download function that updates progress
  download_with_progress() {
    download_single_file "$1"
    # Atomically increment progress
    (
      flock -x 200
      current=$(cat "$progress_file")
      echo $((current + 1)) > "$progress_file"
      completed=$((current + 1))
      percent=$((completed * 100 / num_files))
      printf "\rProgress: %d/%d (%d%%) files processed" "$completed" "$num_files" "$percent"
    ) 200>"$progress_file.lock"
  }
  
  export -f download_with_progress
  export progress_file
  export num_files
  
  # Run parallel without its progress indicators
  cat "$download_list_file" | parallel --no-notice download_with_progress {}
  
  echo -e "\nDownload completed!"
  
  # Clean up progress files
  rm -f "$progress_file" "$progress_file.lock"
else
  echo "No files to download"
fi

# Clean up temporary file
rm -f "$download_list_file"
