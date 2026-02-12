from pandarallel import pandarallel
import brainimagelibrary
import pandas as pd
import requests
from datetime import date
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
import time
import os

from tqdm import tqdm
from pandarallel import pandarallel

pandarallel.initialize(nb_workers=16, progress_bar=True)

tqdm.pandas()

TOKEN_TTL_SECONDS = 15 * 60  # 15 minutes

# Simple in-memory token cache
_token_cache = {
    "token": None,
    "timestamp": 0.0,  # epoch seconds when token was fetched
}


def __load_credentials(path=os.path.expanduser("~/.SPECTRA")):
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
    tuple[str | None, str | None
        A tuple containing (USERNAME, PASSWORD). If a key is not present
        in the file, its value will be returned as ``None``.

    Raises
    ------
    FileNotFoundError
        If the specified credentials file does not exist.
    ValueError
        If a non-comment, non-empty line does not contain an '=' sign.

    Notes
    -----
    Expected file format example::

        # SPECTRA credentials
        USERNAME=john_doe
        PASSWORD=secret123
    """
    creds = {}
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


def login():
    """
    Always request a fresh token from the API using credentials
    from ~/.SPECTRA.
    """
    username, password = __load_credentials()
    if not username or not password:
        raise ValueError("USERNAME and PASSWORD must be defined in ~/.SPECTRA")

    url = "https://storcycle.bil.psc.edu/openapi/tokens"

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
    }

    payload = {
        "username": username,
        "password": password,
    }

    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()

    data = response.json()
    token = data.get("token")
    if not token:
        raise RuntimeError("No 'token' field found in authentication response")

    return token


def __get_token():
    """
    Retrieve a valid authentication token, using a local cache to avoid
    unnecessary reauthentication.

    If a cached token exists and is younger than ``TOKEN_TTL_SECONDS``,
    it is returned immediately. Otherwise, a new token is obtained via
    ``login()``, stored in the cache along with its timestamp, and then
    returned.

    Returns
    -------
    str
        A valid authentication token.

    Notes
    -----
    The function relies on a module-level cache of the form::

        _token_cache = {
            "token": <str or None>,
            "timestamp": <float>
        }

    and a token time-to-live constant::

        TOKEN_TTL_SECONDS = 15 * 60  # example

    The cache is updated whenever a new token is fetched.
    """
    now = time.time()

    # Check if cached token is still valid
    if (
        _token_cache["token"] is not None
        and (now - _token_cache["timestamp"]) < TOKEN_TTL_SECONDS
    ):
        return _token_cache["token"]

    # Need a new token
    new_token = login()
    _token_cache["token"] = new_token
    _token_cache["timestamp"] = now
    return new_token


def exists(dataset_id, token=None):
    """
    Check whether a dataset (project) exists in the StorCycle system.

    The function sends a GET request to the project endpoint using either
    a provided authentication token or a freshly obtained one (via
    ``login()``). A status code of 200 indicates that the dataset exists,
    while a 404 indicates that it does not. Any other status code is
    treated as an unexpected error and results in an exception.

    Parameters
    ----------
    dataset_id : str
        The identifier of the dataset (project) to check.
    token : str, optional
        An authentication token. If not provided, a new token is obtained
        automatically via ``login()``.

    Returns
    -------
    bool
        ``True`` if the dataset exists, ``False`` if it does not.

    Raises
    ------
    requests.HTTPError
        If the API returns a status code other than 200 or 404.
    """
    # Use provided token or fetch one
    if token is None:
        token = login()

    url = f"https://storcycle.bil.psc.edu/openapi/projects/{dataset_id}"
    headers = {"accept": "application/json", "Authorization": f"Bearer {token}"}

    # Perform the GET request
    response = requests.get(url, headers=headers)

    # Project exists
    if response.status_code == 200:
        return True

    # Project not found
    if response.status_code == 404:
        return False

    # Unexpected status code — raise exception
    response.raise_for_status()


def get_projects(take=100):
    """
    Retrieve all projects from the Spectra Logic API using the /projects endpoint.

    Parameters
    ----------
    take : int
        Number of items per page requested from the API (default: 1000).
        The API may cap this value internally (e.g., at 500).

    Returns
    -------
    list
        All retrieved project objects.
    """
    token = __get_token()  # or login()

    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
    }

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
        return response

        # Normalize response format
        if isinstance(page, dict) and "items" in page:
            items = page["items"]
        else:
            items = page

        # Stop if no more results
        if not items:
            print("No more items — done.")
            break

        all_projects.extend(items)

        # Progress message
        print(
            f"Fetched page {page_num}: {len(items)} items "
            f"(total so far: {len(all_projects)})"
        )

        # Increment page counters
        skip += len(items)
        page_num += 1

    print(f"Retrieved {len(all_projects)} total projects.")
    df = pd.json_normalize(all_projects["data"])
    return df


def get(dataset_id, token=None):
    """
    Retrieve a single dataset (project) object from the StorCycle API.

    If no authentication token is supplied, a fresh token is obtained
    using ``login()``. The function performs a GET request to the project
    endpoint and returns the parsed JSON response.

    Parameters
    ----------
    dataset_id : str
        Identifier of the dataset to retrieve.
    token : str, optional
        A valid authentication token. If omitted, ``login()`` is used to
        obtain one.

    Returns
    -------
    dict
        A JSON-decoded dictionary representing the project.

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


from typing import Any, Dict, List, Optional


def jobStatus(limit: int = 500, includeAll: bool = False) -> List[Dict[str, Any]]:
    """
    Fetch all jobStatus rows using OpenAPI-style pagination fields returned by the API:
      - ResultLimit
      - ResultOffset
      - TotalResults
      - data

    We request pages using skip/limit (as this endpoint expects),
    and we advance using ResultOffset/ResultLimit/TotalResults from the response.
    """
    token = login()
    base_url = "https://storcycle.bil.psc.edu/openapi/jobStatus"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {token}",
    }

    all_items: List[Dict[str, Any]] = []
    offset = 0
    total_results: Optional[int] = None

    while True:
        params = {
            "skip": offset,  # request-side offset
            "limit": limit,  # request-side page size
            "includeAll": str(includeAll).lower(),
        }

        resp = requests.get(base_url, headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        payload = resp.json()

        if not isinstance(payload, dict):
            raise ValueError(f"Unexpected response type: {type(payload).__name__}")

        # OpenAPI-ish envelope fields (as observed)
        if total_results is None:
            tr = payload.get("TotalResults")
            if isinstance(tr, int):
                total_results = tr

        result_offset = payload.get("ResultOffset", offset)
        result_limit = payload.get("ResultLimit", limit)

        data = payload.get("data")
        # In your API, `data` may be a list or a dict containing a list.
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            # common OpenAPI-ish patterns
            for k in (
                "items",
                "results",
                "records",
                "rows",
                "jobStatuses",
                "jobStatus",
            ):
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

        # accumulate
        all_items.extend(items)

        # advance offset using what the server says (spec-driven)
        next_offset = int(result_offset) + len(items)

        # stop if server provided TotalResults and we reached it
        if isinstance(total_results, int) and next_offset >= total_results:
            break

        # also stop if server claims a limit but returns fewer items than it claims AND no total is provided
        if total_results is None and len(items) == 0:
            break

        offset = next_offset

        # safety: if server doesn't advance and keeps returning same page
        if offset == result_offset:
            raise RuntimeError(
                "Pagination did not advance (server returned same ResultOffset repeatedly)."
            )

    return all_items


def get_status():
    report = daily()
    report = report[["name", "job", "state", "start", "completion"]]

    report = report.sort_values(by="job")
    today = date.today().strftime("%Y%m%d")
    outfile = f"status-{today}.tsv"
    report.to_csv(outfile, sep="\t", index=False)
    return report


def create(name, description, directory, token=None):
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


def __get_status(token=None, page_size=500):
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

            # Convert the response into a dataframe
            df = pd.DataFrame(payload)

            if df.empty:
                break

            if "data" not in df.columns:
                break

            # Collect items from this page
            page_items = df["data"].tolist()
            all_items.extend(page_items)

            # If fewer than page_size returned → last page
            if len(df) < page_size:
                break

            # Move to next page
            skip += page_size

        # If no data was found, return empty
        if not all_items:
            return pd.DataFrame()

        # Expand ALL rows into a final dataframe
        return __keep_latest_backup(pd.json_normalize(all_items))

    except Exception:
        return pd.DataFrame()


def __keep_latest_backup(df):
    # Split "job" into name + latest_backup
    # rsplit with maxsplit=1 handles names that contain dashes
    df[["name", "latest_backup"]] = df["job"].str.rsplit("-", n=1, expand=True)

    # Convert backup number to integer (errors='coerce' → NaN if not valid)
    df["latest_backup"] = pd.to_numeric(df["latest_backup"], errors="coerce")

    # Keep only the rows with the maximum backup per name
    df_latest = df.loc[df.groupby("name")["latest_backup"].idxmax()].reset_index(
        drop=True
    )

    return df_latest


def login():
    """
    Always request a fresh token from the API using credentials
    from ~/.SPECTRA.
    """
    username, password = __load_credentials()
    if not username or not password:
        raise ValueError("USERNAME and PASSWORD must be defined in ~/.SPECTRA")

    url = "https://storcycle.bil.psc.edu/openapi/tokens"

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
    }

    payload = {
        "username": username,
        "password": password,
    }

    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()

    data = response.json()
    token = data.get("token")
    if not token:
        raise RuntimeError("No 'token' field found in authentication response")

    return token


def _job_status_df(include_all=True, page_size=500):
    """
    Retrieve ALL StorCycle jobStatus entries into a pandas DataFrame, paginating as needed.

    Parameters
    ----------

    include_all : bool
        If True, request includeAll=true (may include completed/older jobs depending on retention).
    page_size : int
        Page size for pagination (API supports limit/skip).

    Returns
    -------
    pandas.DataFrame
        DataFrame of all job status objects returned by the API.
    """

    token = login()

    base_url = "https://storcycle.bil.psc.edu/openapi"
    url = f"{base_url.rstrip('/')}/jobStatus"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {token}",
    }

    all_jobs = []
    skip = 0
    include_all_str = "true" if include_all else "false"

    while True:
        params = {
            "skip": skip,
            "limit": page_size,
            "includeAll": include_all_str,
        }

        resp = requests.get(url, headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        payload = resp.json()

        batch = payload.get("data", [])
        if not batch:
            break

        all_jobs.extend(batch)

        # If fewer than requested, we're done
        if len(batch) < page_size:
            break

        skip += page_size

        # Optional: if API returns a total count, you can stop early
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

    jobs = jobs.drop(columns=["categories"])

    jobs2 = jobs.copy()

    # 2) extract bildid (everything before final -<number>) and backup index (final number)
    extracted = jobs2["job"].str.extract(r"^(?P<bildid>.+)-(?P<backup_idx>\d+)$")
    jobs2["bildid"] = extracted["bildid"]
    jobs2["backup_idx"] = pd.to_numeric(extracted["backup_idx"], errors="coerce")

    # 3) keep only the highest index per bildid
    jobs2 = (
        jobs2.sort_values(
            ["bildid", "backup_idx"]
        )  # ascending so idxmax is last; we'll take last
        .dropna(subset=["bildid", "backup_idx"])
        .drop_duplicates(subset=["bildid"], keep="last")
        .drop(columns=["job"])  # keep bildid if you want; remove if not needed
        .sort_values("bildid")  # or sort_values("bildid") if you prefer
        .reset_index(drop=True)
    )

    jobs2 = jobs2[
        [
            "bildid",
            "backup_idx",
            "state",
            "percentComplete",
            "start",
            "completion",
            "totalFiles",
        ]
    ]
    order = ["Failed", "Canceled", "Completed", "Active"]
    jobs2["state"] = pd.Categorical(jobs2["state"], categories=order, ordered=True)
    jobs2 = jobs2.sort_values("state")

    return jobs2


def daily():
    """
    Generate (or load) the daily BIL report TSV.

    Behavior
    --------
    - If today's TSV (YYYYMMDD.tsv) already exists, load and return it.
    - Otherwise:
        1) Build the daily report dataframe
        2) Keep only rows where exists(bildid) is True
        3) Add StorCycle job state as column 'status' (looked up by bildid)
        4) Save to YYYYMMDD.tsv and return the dataframe
    """
    today = datetime.today().strftime("%Y%m%d")
    output_file = Path(f"{today}.tsv")

    # If file exists, load and return (no API calls)
    if output_file.exists():
        return pd.read_csv(output_file, sep="\t")

    # Generate dataframe
    df = _job_status_df()

    # Save to TSV
    df.to_csv(output_file, sep="\t", index=False)

    return df
