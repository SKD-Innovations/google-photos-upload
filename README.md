# Google Photos Upload Pipeline

This project is a resilient upload pipeline for importing large Google Takeout-style media libraries into Google Photos while preserving metadata (capture date/time, location, and description).

It is designed for long-running, high-volume uploads and can recover from interruptions without reprocessing completed files.

---

## What this tool can do

- Scan selected source folders and build a persistent upload queue.
- Skip hidden files, sidecar JSON files, edited duplicates, and known duplicate media signatures.
- Merge metadata from Google Takeout sidecar JSON files into media files using `exiftool`.
- Upload media to Google Photos through an existing Chrome session via Playwright + CDP.
- Run multiple upload workers in parallel for better throughput.
- Handle large files safely with separate bandwidth throttling behavior.
- Track progress and retries in SQLite so runs can be resumed after failures or interruption.

---

## How it works (high level)

1. Indexes files from configured folders into `gp_pipeline.db`.
2. Copies a batch to local scratch space.
3. Applies metadata from matching JSON sidecars.
4. Uploads batch through Google Photos web UI automation.
5. Marks each file as `COMPLETED`, `FAILED`, or retries as needed.
6. Repeats until queue is empty.

---

## Requirements

- Python 3.10+
- Google Chrome
- `exiftool`
- Python dependency:
  - `playwright`

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Install Playwright browser support (first-time setup):

```bash
python -m playwright install chromium
```

Install `exiftool` using your system package manager (or official distribution for your OS).

---

## Configuration

All configuration is currently in `pipeline2.py` under the **CONFIGURATION** section.

Update these values before running:

- `SOURCE_ROOT`: root folder containing your exported media.
- `TARGET_FOLDERS`: list of top-level folders under `SOURCE_ROOT` to include.
- `SCRATCH_BASE`: local temporary working directory used by workers.
- `DB_PATH`: SQLite state database path.
- `BATCH_SIZE_BYTES`: max batch size per worker.
- `CONCURRENT_UPLOAD_WORKERS`: number of upload workers/pages.
- `EXIF_WORKERS`: parallel metadata processing workers.
- `LARGE_FILE_THRESHOLD_BYTES`: threshold used for large-file handling.

Supported media formats:

- Photos: `.jpg`, `.jpeg`, `.heic`, `.png`, `.webp`, `.gif`
- Videos: `.mp4`, `.mov`, `.m4v`, `.avi`, `.3gp`

---

## Prepare Chrome for upload automation

The script connects to a Chrome instance already running with remote debugging on port `9222`.

Start Chrome with a dedicated profile (example from script output):

```bash
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --remote-debugging-port=9222 --user-data-dir="$HOME/gp_chrome_profile" &
```

Then:

1. Open `https://photos.google.com`
2. Sign in to the Google account you want to upload into
3. Keep this Chrome session running

---

## Usage

From the repository root:

```bash
python pipeline2.py
```

What to expect:

- Initial indexing of configured folders.
- Worker startup and staged batch logs.
- Automatic upload confirmation and retry behavior.
- Final completion message when all queue items are processed.

---

## Resume, retries, and failure behavior

- Queue state is stored in SQLite (`DB_PATH`).
- Files in `STAGED` are reset to `PENDING` when the script restarts.
- Failed batches are re-queued with retry counters.
- Files rejected by Google Photos are marked as `FAILED`.
- Retries stop after configured retry logic (`retry_count < 3` in current code).

This means the pipeline is safe to stop and restart without losing overall progress.

---

## Notes and limitations

- This is UI automation against Google Photos web; UI changes by Google may require selector updates.
- Keep enough free space for scratch copies and metadata writes.
- Large libraries can run for many hours; a stable network connection is recommended.
- The current implementation is configured through code constants, not CLI flags or config files.

---

## Troubleshooting

- **Cannot connect to Chrome on 127.0.0.1:9222**
  - Ensure Chrome is launched with `--remote-debugging-port=9222`.
- **Uploads stall or timeout**
  - Reduce `CONCURRENT_UPLOAD_WORKERS` or `BATCH_SIZE_BYTES`.
- **Metadata not applied**
  - Verify corresponding sidecar JSON files exist and match expected naming patterns.
- **Disk pressure in scratch area**
  - Lower `BATCH_SIZE_BYTES` and ensure `SCRATCH_BASE` points to a volume with enough free space.
