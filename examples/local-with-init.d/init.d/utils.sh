#!/bin/bash

# Copyright 2023 The Vitess Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This file contains utility functions that can be used throughout the
# various examples.
CURRENT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]:-$0}" )" && pwd )"
COMMON_UTILS_PATH="${CURRENT_DIR}/../../common/lib/utils.sh"

# Check if the common utils file exists before sourcing
if [ -f "$COMMON_UTILS_PATH" ]; then
  source "$COMMON_UTILS_PATH"
else
  echo "Error: Cannot find common utils file at $COMMON_UTILS_PATH" >&2
  exit 1
fi

# Check for required binaries
for binary in mysqld etcd etcdctl curl vtctldclient vttablet vtgate vtctld mysqlctl; do
  command -v "$binary" > /dev/null || fail "${binary} is not installed in PATH. See https://vitess.io/docs/get-started/local/ for install instructions."
done;

# These are additional utilities that can be moved back into lib/utils.sh
# At some point. I didn't want to do it right now, because it adds dependencies such as yq,
# and this is not required by local/*
# Note: yq is preferred but not required - functions will fallback to grep-based parsing
# But assuming we bump the docker images, I would like to use yq. It is much safer
# at parsing consistently.

# Configuration file utilities
# Default configuration file location is ~/.vitess.yaml
# It takes precedence over /etc/vitess.yaml
CONFIG_FILE="$HOME/.vitess.yaml"
if [ -f "$CONFIG_FILE" ]; then
  echo "Using configuration file: $CONFIG_FILE"
else
  CONFIG_FILE="/etc/vitess.yaml"
fi

# Function to read YAML configuration
read_yaml_config() {
  if [ ! -f "$CONFIG_FILE" ]; then
    # Try fallback config if default doesn't exist
    echo "Warning: Configuration file $CONFIG_FILE not found, using default values" >&2
    return 1
  fi
  return 0
}

# Function to get configuration value from YAML
# Usage: get_config_value <section> <key> <default_value>
get_config_value() {
  local section=$1
  local key=$2
  local default_value=$3
  
  if ! read_yaml_config; then
    echo "$default_value"
    return
  fi
  
  # Try using yq first if available
  if command -v yq > /dev/null 2>&1; then
    local value=$(yq eval ".$section.$key" "$CONFIG_FILE" 2>/dev/null)
    if [ -z "$value" ] || [ "$value" = "null" ]; then
      echo "$default_value"
    else
      echo "$value"
    fi
    return
  fi
  
  # Fallback to grep-based parsing
  local value=""
  local in_section=false
  local section_indent=""
  
  while IFS= read -r line; do
    # Skip comments and empty lines
    if [[ "$line" =~ ^[[:space:]]*# ]] || [[ "$line" =~ ^[[:space:]]*$ ]]; then
      continue
    fi
    
    # Check if we're entering the target section
    if [[ "$line" =~ ^[[:space:]]*${section}:[[:space:]]*$ ]]; then
      in_section=true
      section_indent=$(echo "$line" | sed 's/[^[:space:]].*//')
      continue
    fi
    
    # If we're in the section, look for the key
    if [ "$in_section" = true ]; then
      # Check if we've left the section (same or less indentation with a key)
      if [[ "$line" =~ ^[[:space:]]*[^[:space:]].*:[[:space:]]* ]]; then
        local current_indent=$(echo "$line" | sed 's/[^[:space:]].*//')
        if [ ${#current_indent} -le ${#section_indent} ] && [[ ! "$line" =~ ^[[:space:]]*${key}:[[:space:]]* ]]; then
          break
        fi
      fi
      
      # Look for our key
      if [[ "$line" =~ ^[[:space:]]*${key}:[[:space:]]*(.*)[[:space:]]*$ ]]; then
        value="${BASH_REMATCH[1]}"
        # Remove quotes if present
        value=$(echo "$value" | sed 's/^["'\'']\(.*\)["'\'']$/\1/')
        break
      fi
    fi
  done < "$CONFIG_FILE"
  
  if [ -z "$value" ]; then
    echo "$default_value"
  else
    echo "$value"
  fi
}

# Function to get tablet UIDs from configuration
# Usage: get_tablet_uids
get_tablet_uids() {
  if ! read_yaml_config; then
    return 1
  fi
  
  # Try using yq first if available
  if command -v yq > /dev/null 2>&1; then
    TABLET_UIDS=$(yq eval '.tablets[].uid' "$CONFIG_FILE" 2>/dev/null)
    if [ -z "$TABLET_UIDS" ] || [ "$TABLET_UIDS" = "null" ]; then
      echo "No tablet UIDs found in configuration file" >&2
      return 1
    fi
    return 0
  fi
  
  # Fallback to grep-based parsing
  local uids=""
  local in_tablets_section=false
  local tablets_indent=""
  
  while IFS= read -r line; do
    # Skip comments and empty lines
    if [[ "$line" =~ ^[[:space:]]*# ]] || [[ "$line" =~ ^[[:space:]]*$ ]]; then
      continue
    fi
    
    # Check if we're entering the tablets section
    if [[ "$line" =~ ^[[:space:]]*tablets:[[:space:]]*$ ]]; then
      in_tablets_section=true
      tablets_indent=$(echo "$line" | sed 's/[^[:space:]].*//')
      continue
    fi
    
    # If we're in the tablets section
    if [ "$in_tablets_section" = true ]; then
      # Check if we've left the tablets section
      if [[ "$line" =~ ^[[:space:]]*[^[:space:]].*:[[:space:]]* ]]; then
        local current_indent=$(echo "$line" | sed 's/[^[:space:]].*//')
        if [ ${#current_indent} -le ${#tablets_indent} ]; then
          break
        fi
      fi
      
      # Look for uid entries
      if [[ "$line" =~ ^[[:space:]]*-[[:space:]]*uid:[[:space:]]*(.*)[[:space:]]*$ ]] || [[ "$line" =~ ^[[:space:]]*uid:[[:space:]]*(.*)[[:space:]]*$ ]]; then
        local uid="${BASH_REMATCH[1]}"
        # Remove quotes if present
        uid=$(echo "$uid" | sed 's/^["'\'']\(.*\)["'\'']$/\1/')
        if [ -n "$uids" ]; then
          uids="$uids"$'\n'"$uid"
        else
          uids="$uid"
        fi
      fi
    fi
  done < "$CONFIG_FILE"
  
  if [ -z "$uids" ]; then
    echo "No tablet UIDs found in configuration file" >&2
    return 1
  fi
  
  TABLET_UIDS="$uids"
  return 0
}

# Function to get tablet configuration by UID
# Usage: get_tablet_config <uid> <key> <default_value>
get_tablet_config() {
  local uid=$1
  local key=$2
  local default_value=$3
  
  if ! read_yaml_config; then
    echo "$default_value"
    return
  fi
  
  # Try using yq first if available
  if command -v yq > /dev/null 2>&1; then
    local value=$(yq eval ".tablets[] | select(.uid == $uid) | .$key" "$CONFIG_FILE" 2>/dev/null)
    if [ -z "$value" ] || [ "$value" = "null" ]; then
      echo "$default_value"
    else
      echo "$value"
    fi
    return
  fi
  
  # Fallback to grep-based parsing
  local value=""
  local in_tablets_section=false
  local in_target_tablet=false
  local tablets_indent=""
  local tablet_indent=""
  
  while IFS= read -r line; do
    # Skip comments and empty lines
    if [[ "$line" =~ ^[[:space:]]*# ]] || [[ "$line" =~ ^[[:space:]]*$ ]]; then
      continue
    fi
    
    # Check if we're entering the tablets section
    if [[ "$line" =~ ^[[:space:]]*tablets:[[:space:]]*$ ]]; then
      in_tablets_section=true
      tablets_indent=$(echo "$line" | sed 's/[^[:space:]].*//')
      continue
    fi
    
    # If we're in the tablets section
    if [ "$in_tablets_section" = true ]; then
      # Check if we've left the tablets section
      if [[ "$line" =~ ^[[:space:]]*[^[:space:]].*:[[:space:]]* ]]; then
        local current_indent=$(echo "$line" | sed 's/[^[:space:]].*//')
        if [ ${#current_indent} -le ${#tablets_indent} ]; then
          break
        fi
      fi
      
      # Look for tablet entries (starting with -)
      if [[ "$line" =~ ^[[:space:]]*-[[:space:]]* ]]; then
        in_target_tablet=false
        tablet_indent=$(echo "$line" | sed 's/[^[:space:]].*//')
      fi
      
      # Check if this tablet has the target UID
      if [[ "$line" =~ ^[[:space:]]*-[[:space:]]*uid:[[:space:]]*(.*)[[:space:]]*$ ]] || [[ "$line" =~ ^[[:space:]]*uid:[[:space:]]*(.*)[[:space:]]*$ ]]; then
        local found_uid="${BASH_REMATCH[1]}"
        # Remove quotes if present
        found_uid=$(echo "$found_uid" | sed 's/^["'\'']\(.*\)["'\'']$/\1/')
        if [ "$found_uid" = "$uid" ]; then
          in_target_tablet=true
        fi
      fi
      
      # If we're in the target tablet, look for the key
      if [ "$in_target_tablet" = true ]; then
        if [[ "$line" =~ ^[[:space:]]*${key}:[[:space:]]*(.*)[[:space:]]*$ ]]; then
          value="${BASH_REMATCH[1]}"
          # Remove quotes if present
          value=$(echo "$value" | sed 's/^["'\'']\(.*\)["'\'']$/\1/')
          break
        fi
      fi
    fi
  done < "$CONFIG_FILE"
  
  if [ -z "$value" ]; then
    echo "$default_value"
  else
    echo "$value"
  fi
}

# Get VTDATAROOT from config and expand tilde if present
vtdataroot_raw=$(get_config_value "global" "vtdataroot" "~/vtdataroot")
VTDATAROOT="${vtdataroot_raw/#\~/$HOME}"

# Export VTDATAROOT so that mysqlctl and other Vitess binaries can see it
export VTDATAROOT

hostname=$(get_config_value "global" "hostname" "localhost")
mkdir -p "$VTDATAROOT/tmp" || { echo "Failed to create tmp directory"; exit 1; }
