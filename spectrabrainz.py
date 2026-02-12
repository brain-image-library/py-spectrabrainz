#!/usr/bin/env python3
"""
spectrabrainz.py

Copyright (c) 2026 Pittsburgh Supercomputing Center (PSC),
Brain Image Library (BIL)

Author: icaoberg

Description:
    Utilities for generating daily status reports for Spectra Logic StorCycle
    "ScanAndArchive" jobs used by the Brain Image Library (BIL).

    This module focuses on:
      - Authenticating to the StorCycle OpenAPI endpoint
      - Fetching job status information with pagination
      - Producing a daily TSV report (YYYYMMDD.tsv)
      - Optional helpers for checking/creating projects and exporting status

Notes / Caveats:
    - This file currently contains some legacy/duplicate code paths (e.g., two
      `login()` definitions) and a likely bug in `get_projects()` that returns
      early. Those are preserved for minimal disruption but are marked with
      FIXME comments below.
    - Credentials are read from ~/.SPECTRA as KEY=VALUE lines.
    - Token caching is in-memory only; cache resets on process restart.
"""

from __future__ import annotations

import os
import re
import subprocess
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from pandarallel import pandarallel
from tqdm import tqdm

# NOTE: Imported but not used in the pasted code. Kept for compatibility in case
# other functions rely on it in your environment.
import brainimagelibrary  # noqa: F401


# ---------------------------------------------------------------------
# Parallelization / progress configuration
# ---------------------------------------------------------------------
pandarallel.initialize(nb_workers=16, progress_bar=True)
tqdm.pandas()

# ---------------------------------------------------------------------
# Authentication token cache
# ---------------------------------------------------------------------
TOKEN_TTL_SECONDS = 15 * 60  # 15 minutes

# Simple in-memory token cache
_token_cache: Dict[str, Any] = {
    "token": None,
    "timestamp": 0.0,  # epoch seconds when token was fetched
}


def __load_credentials(path: str = os.path.expanduser("~/.SPECTRA")) -> Tuple[Optional[str], Optional[str]]:
    """
    Load username and password credentials from a key-value file.

    The credentials file must contain lines in the format:
        KEY=VALUE
    Blank lines and lines beginning with '#' are ignored.

    Parameters
    ----------
    path : str, optional
        Path to the credentials file. Defaults to ``~/.SPECTRA``.

    Returns
    -------
    (USERNAME, PASSWORD) : tuple[str | None, str | None]
        A tuple containing (USERNAME, PASSWORD). If a key is not present
        in the file, its value will be returned as ``None``.

    Raises
    ------
    FileNotFoundError
        If the specified credentials file does not exist.
    ValueError
        If a non-comment, non-empty line does not contain an '=' sign.

    Example
    -------
    # SPECTRA credentials
    USERNAME=john_doe
    PASSWORD=secret123
    """
    creds: Dict[str, str] = {}
    if not os.path.exists(path):
        raise FileNotFoundError(f"Credential file not found: {path}")

    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                raise ValueError(f"Invalid line in credentials file: {line}")
            key, value = line.split("=", 1)
            creds[key.strip()] = value.strip()

    return creds.get("USERNAME"), creds.get("PASSWORD")


def login() -> str:
    """
    Request a fresh authentication token from the StorCycle API using
    credentials from ~/.SPECTRA.

    Returns
    -------
    str
        Bearer token for Authorization header.

    Raises
    ------
    ValueError
        If USERNAME/PASSWORD are missing from the credentials file.
    requests.HTTPError
        If the API request fails.
    RuntimeError
        If the response JSON does not contain a 'token' field.
    """
    username, password = __load_credentials()
    if not username or not password:
        raise ValueError("USERNAME and PASSWORD must be defined in ~/.SPECTRA")

    url = "https://storcycle.bil.psc.edu/openapi/tokens"
    headers = {"accept": "application/json", "Content-Type": "application/json"}
    payload = {"username": username, "password": password}

    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()

    data = response.json()
    token = data.get("token")
    if not token:
        raise RuntimeError("No 'token' field found in authentication response")

    return token


def __get_token() -> str:
    """
    Retrieve a valid authentication token using a local in-memory cache.

    If a cached token exists and is younger than TOKEN_TTL_SECONDS, it is
    returned. Otherwise, a new token is obtained via login() and cached.

    Returns
    -------
    str
        A valid authentication token.
    """
    now = time.time()

    if (
        _token_cache["token"] is not None
        and (now - _token_cache["timestamp"]) < TOKEN_TTL_SECONDS
    ):
        return _token_cache["token"]

    new_token = login()
    _token_cache["token"] = new_token
    _token_cache["timestamp"] = now
    return new_token


def exists(dataset_id: str, token: Optional[str] = None) -> bool:
    """
    Check whether a dataset (project) exists in the StorCycle system.

    Parameters
    ----------
    dataset_id : str
        Identifier of the dataset (project) to check.
    token : str, optional
        Authentication token. If not provided, login() is used.

    Returns
    -------
    bool
        True if the dataset exists, False if it does not.

    Raises
    ------
    requests.HTTPError
        If the API returns a status code other than 200 or 404.
    """
    if token is None:
        token = login()

    url = f"https://storcycle.bil.psc.edu/openapi/projects/{dataset_id}"
    headers = {"accept": "application/json", "Authorization": f"Bearer {token}"}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return True
    if response.status_code == 404:
        return False
    response.raise_for_status()
    return False  # unreachable, but keeps type-checkers happy


def get_projects(take: int = 100):
    """
    Retrieve projects from the StorCycle API using the /projects endpoint.

    Parameters
    ----------
    take : int
        Number of items per page requested from the API (default: 100).

    Returns
    -------
    pandas.DataFrame | list
        Intended: all retrieved project objects (normalized to a DataFrame).
        Current behavior: returns a `requests.Response` early due to a legacy
        `return response` statement (see FIXME below).

    FIXME
    -----
    The current function returns `response` immediately after fetching the
    first page, which prevents pagination and the final dataframe creation.
    """
    token = __get_token()

    headers = {"Authorization": f"Bearer {token}", "accept": "application/json"}

    all_projects = []
    skip = 0
    page_num = 1

    while True:
        url = (
            "https://storcycle.bil.psc.edu/openapi/projects"
            f"?skip={skip}&limit={take}&active=true&filterBy=ScanAndArchive"
        )

        response = requests.get(url, headers=headers)
        response.raise_for_status()
        page = response.json()

        # FIXME: This appears to be an accidental early return in the pasted code.
        # Keeping it for minimal behavior change.
        return response

        if isinstance(page, dict) and "items" in page:
            items = page["items"]
        else:
            items = page

        if not items:
            print("No more items — done.")
            break

        all_projects.extend(items)

        print(
            f"Fetched page {page_num}: {len(items)} items "
            f"(total so far: {len(all_projects)})"
        )

        skip += len(items)
        page_num += 1

    print(f"Retrieved {len(all_projects)} total projects.")
    # NOTE: all_projects is a list; indexing ["data"] would be invalid unless
    # you intended a dict structure. Preserved as-is but likely needs adjustment.
    df = pd.json_normalize(all_projects["data"])  # type: ignore[index]
    return df


def get(dataset_id: str, token: Optional[str] = None) -> Dict[str, Any]:
    """
    Retrieve a single dataset (project) object from the StorCycle API.

    Parameters
    ----------
    dataset_id : str
        Identifier of the dataset to retrieve.
    token : str, optional
        Authentication token. If omitted, login() is used.

    Returns
    -------
    dict
        JSON-decoded project representation.

    Raises
    ------
    requests.HTTPError
        If the API returns a non-2xx HTTP status code.
    """
    if token is None:
        token = login()

    url = f"https://storcycle.bil.psc.edu/openapi/projects/{dataset_id}"
    headers = {"accept": "application/json", "Authorization": f"Bearer {token}"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def jobStatus(limit: int = 500, includeAll: bool = False) -> List[Dict[str, Any]]:
    """
    Fetch all jobStatus rows using OpenAPI-style pagination fields returned by the API.

    The API is queried with skip/limit parameters. The response is expected to
    include (some of) the following keys:
      - ResultLimit
      - ResultOffset
      - TotalResults
      - data (list or envelope containing a list)

    Parameters
    ----------
    limit : int
        Page size for requests.
    includeAll : bool
        Whether to include all jobs (true) vs filtered/recent only (false).

    Returns
    -------
    list[dict[str, Any]]
        Aggregated list of job status entries.
    """
    token = login()
    base_url = "https://storcycle.bil.psc.edu/openapi/jobStatus"
    headers = {"accept": "application/json", "Authorization": f"Bearer {token}"}

    all_items: List[Dict[str, Any]] = []
    offset = 0
    total_results: Optional[int] = None

    while True:
        params = {
            "skip": offset,
            "limit": limit,
            "includeAll": str(includeAll).lower(),
        }

        resp = requests.get(base_url, headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        payload = resp.json()

        if not isinstance(payload, dict):
            raise ValueError(f"Unexpected response type: {type(payload).__name__}")

        if total_results is None:
            tr = payload.get("TotalResults")
            if isinstance(tr, int):
                total_results = tr

        result_offset = payload.get("ResultOffset", offset)
        result_limit = payload.get("ResultLimit", limit)

        data = payload.get("data")
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            for k in ("items", "results", "records", "rows", "jobStatuses", "jobStatus"):
                v = data.get(k)
                if isinstance(v, list):
                    items = v
                    break
            else:
                raise ValueError(
                    f"Could not find list inside payload['data'] keys={list(data.keys())}"
                )
        else:
            raise ValueError(
                f"payload['data'] is neither list nor dict; type={type(data).__name__}"
            )

        if not items:
            break

        all_items.extend(items)

        next_offset = int(result_offset) + len(items)

        if isinstance(total_results, int) and next_offset >= total_results:
            break

        offset = next_offset

        if offset == result_offset:
            raise RuntimeError(
                "Pagination did not advance (server returned same ResultOffset repeatedly)."
            )

        # `result_limit` is unused but kept for readability/traceability.
        _ = result_limit

    return all_items


def get_status() -> pd.DataFrame:
    """
    Generate the daily dataframe (via daily()), write a status-YYYYMMDD.tsv file,
    and return the dataframe.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns: name, job, state, start, completion (if present).
    """
    report = daily()

    # NOTE: daily() currently returns a reduced schema (see _job_status_df()).
    # This column selection may fail if columns are not present.
    report = report[["name", "job", "state", "start", "completion"]]

    report = report.sort_values(by="job")
    today = date.today().strftime("%Y%m%d")
    outfile = f"status-{today}.tsv"
    report.to_csv(outfile, sep="\t", index=False)
    return report


def create(name: str, description: str, directory: str, token: Optional[str] = None) -> Dict[str, Any]:
    """
    Create an archive project in StorCycle via PUT /projects/archive/{name}.

    Parameters
    ----------
    name : str
        Project name.
    description : str
        Project description.
    directory : str
        Working directory path.
    token : str, optional
        Authentication token. If None, the request will likely fail unless
        the caller supplies a valid token.

    Returns
    -------
    dict
        JSON response from the API.
    """
    url = f"https://storcycle.bil.psc.edu/openapi/projects/archive/{name}"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    payload = {
        "description": description,
        "share": "BIL Published Data",
        "projectType": "ScanAndArchive",
        "workingDirectory": directory,
        "targets": ["BIL Published Data on Tape"],
        "active": True,
        "enabled": True,
        "breadCrumbAction": "KeepOriginal",
        "delayedActionDays": 0,
        "filter": {
            "minimumAge": "AnyAge",
            "customAgeInDays": 0,
            "minimumSize": "Any",
        },
        "schedule": {"period": "Now"},
    }

    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()


def __get_status(token: Optional[str] = None, page_size: int = 500) -> pd.DataFrame:
    """
    Legacy helper to fetch job status and return a normalized DataFrame,
    keeping only the latest backup per dataset.

    Parameters
    ----------
    token : str, optional
        Authentication token; if None, login() is used.
    page_size : int
        Pagination size.

    Returns
    -------
    pandas.DataFrame
        Normalized dataframe of job statuses, reduced to latest backup per name.
        Returns an empty dataframe on errors.
    """
    try:
        if token is None:
            token = login()

        all_items = []
        skip = 0
        headers = {"accept": "application/json", "Authorization": f"Bearer {token}"}

        while True:
            url = (
                "https://storcycle.bil.psc.edu/openapi/jobStatus"
                f"?skip={skip}&limit={page_size}"
                "&sortBy=name&sortType=ASC"
                "&filterBy=ScanAndArchive&includeAll=false"
            )

            response = requests.get(url, headers=headers)
            response.raise_for_status()
            payload = response.json()

            # NOTE: The original code wraps payload directly into DataFrame.
            # That typically produces columns from keys, not rows. Preserved.
            df = pd.DataFrame(payload)

            if df.empty:
                break
            if "data" not in df.columns:
                break

            page_items = df["data"].tolist()
            all_items.extend(page_items)

            if len(df) < page_size:
                break

            skip += page_size

        if not all_items:
            return pd.DataFrame()

        return __keep_latest_backup(pd.json_normalize(all_items))

    except Exception:
        return pd.DataFrame()


def __keep_latest_backup(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reduce a job-status dataframe to only the latest backup per dataset name.

    Assumes df has a 'job' column containing strings formatted like:
        <name>-<backup_number>

    Returns
    -------
    pandas.DataFrame
        Filtered dataframe containing only the row with max backup number per name.
    """
    df[["name", "latest_backup"]] = df["job"].str.rsplit("-", n=1, expand=True)
    df["latest_backup"] = pd.to_numeric(df["latest_backup"], errors="coerce")

    df_latest = df.loc[df.groupby("name")["latest_backup"].idxmax()].reset_index(drop=True)
    return df_latest


# ---------------------------------------------------------------------
# NOTE: Duplicate login() in pasted code
# ---------------------------------------------------------------------
# FIXME:
# The original script defines `login()` twice (identical). That is redundant
# and the second definition overwrites the first. We keep only one definition
# above for clarity and to avoid confusion.


def _job_status_df(include_all: bool = True, page_size: int = 500) -> pd.DataFrame:
    """
    Retrieve StorCycle jobStatus entries into a pandas DataFrame, paginating as needed.

    Parameters
    ----------
    include_all : bool
        If True, request includeAll=true (may include completed/older jobs).
    page_size : int
        Page size for pagination (API supports limit/skip).

    Returns
    -------
    pandas.DataFrame
        DataFrame of job status objects returned by the API, filtered and reduced to
        the latest backup index per BIL dataset id (bildid).
    """
    token = login()

    base_url = "https://storcycle.bil.psc.edu/openapi"
    url = f"{base_url.rstrip('/')}/jobStatus"
    headers = {"accept": "application/json", "Authorization": f"Bearer {token}"}

    all_jobs = []
    skip = 0
    include_all_str = "true" if include_all else "false"

    while True:
        params = {"skip": skip, "limit": page_size, "includeAll": include_all_str}

        resp = requests.get(url, headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        payload = resp.json()

        batch = payload.get("data", [])
        if not batch:
            break

        all_jobs.extend(batch)

        if len(batch) < page_size:
            break

        skip += page_size

        total = payload.get("total")
        if isinstance(total, int) and skip >= total:
            break

    jobs = pd.DataFrame(all_jobs)

    jobs = (
        jobs[
            ~jobs["job"].str.contains(
                r"Daily-Storcycle-Database-Backup|test|Scan|Daily|Restore",
                case=False,
                na=False,
            )
        ]
        .sort_values(by="job")
        .reset_index(drop=True)
    )

    # Some API responses may not include categories; this will raise if missing.
    # Preserved; adjust to `errors="ignore"` if desired.
    jobs = jobs.drop(columns=["categories"])

    jobs2 = jobs.copy()

    extracted = jobs2["job"].str.extract(r"^(?P<bildid>.+)-(?P<backup_idx>\d+)$")
    jobs2["bildid"] = extracted["bildid"]
    jobs2["backup_idx"] = pd.to_numeric(extracted["backup_idx"], errors="coerce")

    jobs2 = (
        jobs2.sort_values(["bildid", "backup_idx"])
        .dropna(subset=["bildid", "backup_idx"])
        .drop_duplicates(subset=["bildid"], keep="last")
        .drop(columns=["job"])
        .sort_values("bildid")
        .reset_index(drop=True)
    )

    jobs2 = jobs2[
        ["bildid", "backup_idx", "state", "percentComplete", "start", "completion", "totalFiles"]
    ]

    order = ["Failed", "Canceled", "Completed", "Active"]
    jobs2["state"] = pd.Categorical(jobs2["state"], categories=order, ordered=True)
    jobs2 = jobs2.sort_values("state")

    return jobs2


def daily() -> pd.DataFrame:
    """
    Generate (or load) the daily BIL report TSV.

    Behavior
    --------
    - If today's TSV (YYYYMMDD.tsv) already exists, load and return it.
    - Otherwise:
        1) Build the daily report dataframe from StorCycle job status
        2) Save to YYYYMMDD.tsv
        3) Return the dataframe

    Returns
    -------
    pandas.DataFrame
        Daily report dataframe (schema defined by _job_status_df()).
    """
    today = datetime.today().strftime("%Y%m%d")
    output_file = Path(f"{today}.tsv")

    if output_file.exists():
        return pd.read_csv(output_file, sep="\t")

    df = _job_status_df()
    df.to_csv(output_file, sep="\t", index=False)
    return df