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
#   - daily.py in the same directory; upload_to_gdrive.py in scripts/
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
/bil/users/icaoberg/miniconda3/bin/python ./daily.py

# Upload results to Google Drive
/bil/users/icaoberg/miniconda3/bin/python ./scripts/upload_to_gdrive.py

# Backup all TSV files matching the 2026*tsv pattern (compressed)
for f in data/2026*tsv; do
    tar -czf "${f}.tar.gz" "$f"
done
rsync -ruv data/2026*tsv.tar.gz /bil/users/icaoberg/backups/spectranbrainz/
rm -f data/2026*tsv.tar.gz

# Backup the Excel report (compressed)
tar -czf spectrabrainz-report.xlsx.tar.gz spectrabrainz-report.xlsx
rsync -ruv spectrabrainz-report.xlsx.tar.gz /bil/users/icaoberg/backups/spectranbrainz/
rm -f spectrabrainz-report.xlsx.tar.gz

# On the last day of the month, copy the report with a YYYYMM-stamped filename and remove the original
today=$(date +%d)
last_day=$(date -d "$(date +%Y-%m-01) +1 month -1 day" +%d)
if [ "$today" -eq "$last_day" ]; then
    stamp=$(date +%Y%m)
    cp spectrabrainz-report.xlsx "spectrabrainz-report.${stamp}.xlsx"
    rm spectrabrainz-report.xlsx
fi

echo "SpectraBrainz daily pipeline completed successfully."