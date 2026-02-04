#!/bin/bash
#
# Loads the PLATFORMS array from platforms.conf.
# Source this file from other scripts: source "$(dirname "$0")/platforms.sh"
#

PLATFORMS_CONF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/platforms.conf"
PLATFORMS=()
while IFS='=' read -r key value; do
    [[ "$key" =~ ^#.*$ || -z "$key" ]] && continue
    PLATFORMS+=("$value")
done < "$PLATFORMS_CONF"
