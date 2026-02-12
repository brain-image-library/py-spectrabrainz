#!/usr/bin/env bash
#
# daily.sh
#
# Copyright (c) 2026 Pittsburgh Supercomputing Center (PSC),
# Brain Image Library (BIL)
#
# Author: icaoberg
#
# Description:
#   This script runs the daily SpectraBrainz maintenance pipeline:
#     1. Executes the daily data processing script (daily.py)
#     2. Uploads results to Google Drive (upload_to_gdrive.py)
#     3. Backs up generated TSV files and the Excel report to the
#        Brain Image Library backup location
#
# Usage:
#   ./daily.sh
#
# Requirements:
#   - Bash
#   - Python available in PATH
#   - daily.py and upload_to_gdrive.py in the same directory
#   - rsync available
#   - Write access to /bil/users/icaoberg/backups/spectranbrainz/
#
# Notes:
#   - The pattern "2026*tsv" assumes TSV files are named with a 2026 prefix.
#   - Adjust paths or filenames as needed for future years or deployments.
#

# Exit immediately if a command fails, a variable is undefined, or a pipeline fails
set -euo pipefail

# Run daily data processing
python ./daily.py

# Upload results to Google Drive
python ./upload_to_gdrive.py

# Backup all TSV files matching the 2026*tsv pattern
rsync -ruv 2026*tsv /bil/users/icaoberg/backups/spectranbrainz/

# Backup the Excel report
rsync -ruv spectrabrainz-report.xlsx /bil/users/icaoberg/backups/spectranbrainz/

echo "SpectraBrainz daily pipeline completed successfully."