#!/usr/bin/env python3
"""
daily.py

Copyright (c) 2026 Pittsburgh Supercomputing Center (PSC),
Brain Image Library (BIL)

Author: icaoberg

Description:
    This script runs the daily SpectraBrainz reporting pipeline by invoking
    the `daily()` function from the `spectrabrainz` module. The function is
    expected to generate and/or update the daily report artifacts used by
    downstream processes (e.g., uploads and backups).

Usage:
    python ./daily.py

Requirements:
    - Python 3
    - The `spectrabrainz` module must be installed and importable
    - Any dependencies required by `spectrabrainz.daily()`

Notes:
    - This script is intentionally minimal and acts as a thin wrapper
      around the SpectraBrainz daily workflow.
"""

import spectrabrainz


def main():
    """Run the SpectraBrainz daily report generation."""
    spectrabrainz.daily()


if __name__ == "__main__":
    main()