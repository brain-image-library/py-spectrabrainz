# SpectraBrainz (StorCycle → Daily TSV + Excel Report)

![](./images/image.png)

Utilities and scripts used by the **Pittsburgh Supercomputing Center (PSC)** / **Brain Image Library (BIL)** to query **Spectra Logic StorCycle** job status via the StorCycle OpenAPI endpoint and produce:

- A **daily TSV** report named `YYYYMMDD.tsv` with columns: `bildid`, `backup_idx`, `state`, `percentComplete`, `start`, `completion`, `totalFiles`, `directory`
- A **status TSV** report named `status-YYYYMMDD.tsv` (subset view)
- An **Excel workbook** `spectrabrainz-report.xlsx` with one sheet per day, sorted and color-formatted, plus a **Histogram of states** chart sheet
- Optional upload of the Excel workbook to **Google Drive** via `rclone`

---

## Contents

- `spectrabrainz.py` — Python module that authenticates to StorCycle, fetches `jobStatus`, and generates daily TSVs.
- `daily.py` — thin wrapper that runs `spectrabrainz.daily()`.
- `upload_to_gdrive.py` — builds/updates `spectrabrainz-report.xlsx` from `YYYYMMDD.tsv` files and uploads it via `rclone`.
- `daily.sh` — daily pipeline runner (generate → upload → compressed rsync backups → end-of-month archiving).

---

## Requirements

- Python 3
- `requests`
- `pandas`
- `openpyxl`
- `matplotlib`
- `tqdm`
- `pandarallel`
- `brainimagelibrary`
- `rclone` (only if using the upload step)
- Network access to:
  - `https://storcycle.bil.psc.edu/openapi/...`

---

## Credentials

Authentication uses a simple key-value file at:

- `~/.SPECTRA`

Format:

```ini
# StorCycle credentials for SpectraBrainz scripts
USERNAME=your_username
PASSWORD=your_password
```

---

## Key behaviors

### spectrabrainz.py

- **Token caching** — authentication tokens are cached in-memory for 15 minutes to reduce login calls.
- **Parallel enrichment** — uses `pandarallel` (16 workers) to fetch `workingDirectory` for each dataset concurrently.
- **Job filtering** — system/maintenance jobs are excluded from reports (matches: `Daily-Storcycle-Database-Backup`, `test`, `Scan`, `Daily`, `Restore`).
- **Latest backup only** — for datasets with multiple backup runs, only the most recent backup index is kept.
- **State ordering** — rows are sorted: Failed → Canceled → Completed → Active.

Public functions:

| Function | Description |
|---|---|
| `login()` | Request a fresh token from the StorCycle API |
| `exists(dataset_id)` | Check whether a dataset project exists |
| `get(dataset_id)` | Retrieve a single dataset object |
| `get_projects()` | Retrieve all active ScanAndArchive projects |
| `jobStatus()` | Fetch all job status rows with pagination |
| `get_status()` | Write `status-YYYYMMDD.tsv` and return a DataFrame |
| `create(name, description, directory)` | Create a ScanAndArchive project |
| `scan(name, description, directory)` | Create a Scan project |
| `daily()` | Generate (or load) the daily `YYYYMMDD.tsv` report |

### upload_to_gdrive.py

- Scans the working directory for `YYYYMMDD.tsv` files and writes one sheet per date into `spectrabrainz-report.xlsx`.
- Rows are sorted by `completion` date (descending) within each sheet.
- Row color coding:

  | State | Color |
  |---|---|
  | Completed | Green (`#228B22`) |
  | Failed | Red (`#B22222`) |
  | Canceled | Yellow (`#FFD700`) |
  | Queued/other | Gray (`#808080`) |

- A **"Histogram of states"** sheet is inserted as the first sheet, containing a stacked bar chart showing state counts per day over time.
- After formatting, the workbook is uploaded to Google Drive via `rclone` at `PSC:Brain_Image_Library/spectrabrainz/`.

### daily.sh

1. Runs `daily.py` to generate today's TSV.
2. Runs `upload_to_gdrive.py` to update and upload the Excel report.
3. Compresses each `YYYYMMDD.tsv` as a `.tar.gz` and rsyncs to `/bil/users/icaoberg/backups/spectranbrainz/`.
4. Compresses and rsyncs `spectrabrainz-report.xlsx` to the same backup location.
5. On the **last day of the month**, copies the Excel report as `spectrabrainz-report.YYYYMM.xlsx` and removes the original.

---

Copyright © 2026 Pittsburgh Supercomputing Center. All Rights Reserved.

The [Biomedical Applications Group](https://www.psc.edu/biomedical-applications/) at the [Pittsburgh Supercomputing
Center](http://www.psc.edu) in the [Mellon College of Science](https://www.cmu.edu/mcs/) at [Carnegie Mellon University](http://www.cmu.edu).
