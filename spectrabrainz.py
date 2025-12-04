import brainimagelibrary
import pandas as pd
import requests
from datetime import date
import os

from tqdm import tqdm
from pandarallel import pandarallel
pandarallel.initialize(nb_workers=16, progress_bar=True)

TOKEN_TTL_SECONDS = 15 * 60  # 15 minutes

# Simple in-memory token cache
_token_cache = {
    "token": None,
    "timestamp": 0.0,  # epoch seconds when token was fetched
}

def __load_credentials(path=os.path.expanduser("~/.SPECTRA")):
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

def __get_token():
    """
    Return a cached token if it is less than 15 minutes old.
    Otherwise, request a new token and update the cache.
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

def exists(dataset_id, token=None):
    # Use provided token or fetch one
    if token is None:
        token = login()

    url = f"https://storcycle.bil.psc.edu/openapi/projects/{dataset_id}"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {token}"
    }

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

def get(dataset_id, token=None):
    # Use provided token or fetch one
    if token is None:
        token = login()

    url = f"https://storcycle.bil.psc.edu/openapi/projects/{dataset_id}"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {token}"
    }

    # Perform request
    response = requests.get(url, headers=headers)

    # Raise exception for any non-2xx response
    response.raise_for_status()

    # Return parsed JSON object
    return response.json()

def daily():
    return Nonec

def get_status():
    report = status()
    report = report[['name', 'job', 'state', 'start', 'completion']]

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
        "projectType": 'ScanAndArchive',
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
        "schedule": {
            "period": "Now"
        }
    }

    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()

def status(token=None, page_size=500):
    try:
        if token is None:
            token = login()

        all_items = []
        skip = 0

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {token}"
        }

        while True:
            url = (
                "https://storcycle.bil.psc.edu/openapi/jobStatus"
                f"?skip={skip}&limit={page_size}"
                "&sortBy=name&sortType=ASC"
                "&filterBy=ScanAndArchive&filterBy=&includeAll=false"
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
    df_latest = df.loc[df.groupby("name")["latest_backup"].idxmax()].reset_index(drop=True)

    return df_latest
