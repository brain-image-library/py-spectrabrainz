# SpectraBrainz (StorCycle → Daily TSV + Excel Report)

![]('./images/image.png')

Utilities and scripts used by the **Pittsburgh Supercomputing Center (PSC)** / **Brain Image Library (BIL)** to query **Spectra Logic StorCycle** job status via the StorCycle OpenAPI endpoint and produce:

- A **daily TSV** report named `YYYYMMDD.tsv`
- A **status TSV** report named `status-YYYYMMDD.tsv` (subset view)
- An **Excel workbook** `spectrabrainz-report.xlsx` with one sheet per day, sorted and color-formatted
- Optional upload of the Excel workbook to **Google Drive** via `rclone`

---

## Contents

- `spectrabrainz.py` — Python module that authenticates to StorCycle, fetches `jobStatus`, and generates daily TSVs.
- `spectrabrainz_daily.py` — thin wrapper that runs `spectrabrainz.daily()`.
- `spectrabrainz_report_uploader.py` — builds/updates `spectrabrainz-report.xlsx` from `YYYYMMDD.tsv` files and uploads it via `rclone`.
- `spectrabrainz_daily.sh` — example daily pipeline runner (generate → upload → rsync backups).

> Filenames above reflect the “documented” versions shown in chat. If your repo uses different names, adjust accordingly.

---

## Requirements

- Python 3
- `requests`
- `pandas`
- `openpyxl`
- `tqdm`
- `pandarallel`
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

---

© 2026 icaoberg  AT Pittsburgh Supercomputing Center (PSC), Brain Image Library (BIL). All rights reserved.