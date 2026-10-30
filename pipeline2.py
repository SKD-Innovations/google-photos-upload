import os
import re
import sys
import json
import shutil
import sqlite3
import asyncio
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor
from playwright.async_api import async_playwright

# ==============================================================================
# CONFIGURATION
# ==============================================================================
SOURCE_ROOT = Path("/Volumes/Sarthak 1TB/sarthak/Backup March 2026/Power AI/Temp")

TARGET_FOLDERS = [
    "Sem 4",
    "Sem 5",
    "Sem 6",
    "Sem 7",
    "Sem 8",
]

SCRATCH_BASE = Path.home() / "gp_scratch_buffer"
DB_PATH = Path.home() / "gp_pipeline.db"

# 3 GB per batch. 2 parallel workers = ~6 GB active SSD usage.
BATCH_SIZE_BYTES = int(3 * 1024 * 1024 * 1024)
CONCURRENT_UPLOAD_WORKERS = 2
EXIF_WORKERS = 4

# Files up to 10 GB are staged to SSD (10 GB file + 10 GB ExifTool temp = 20 GB <= 25 GB SSD budget).
# Files larger than 10 GB stay on HDD.
LARGE_FILE_THRESHOLD_BYTES = int(10 * 1024 * 1024 * 1024)  # 10 GB


PHOTO_EXTS = {".jpg", ".jpeg", ".heic", ".png", ".webp", ".gif"}
VIDEO_EXTS = {".mp4", ".mov", ".m4v", ".avi", ".3gp"}
ALL_SUPPORTED_EXTS = PHOTO_EXTS | VIDEO_EXTS

# ==============================================================================
# 2. FAST JSON RESOLUTION WITH DIRECTORY CACHE
# ==============================================================================
_FOLDER_HAS_JSON_CACHE = {}

def folder_has_json(folder: Path) -> bool:
    """Checks once per folder if any JSON sidecars exist to eliminate HDD globbing."""
    folder_str = str(folder)
    if folder_str not in _FOLDER_HAS_JSON_CACHE:
        try:
            has_json = any(folder.glob("*.json"))
            _FOLDER_HAS_JSON_CACHE[folder_str] = has_json
        except OSError:
            _FOLDER_HAS_JSON_CACHE[folder_str] = False
    return _FOLDER_HAS_JSON_CACHE[folder_str]


# ==============================================================================
# DATABASE ENGINE
# ==============================================================================
def init_db():
    conn = sqlite3.connect(DB_PATH, timeout=60.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS queue (
            rel_path TEXT PRIMARY KEY,
            full_path TEXT,
            size_bytes INTEGER,
            status TEXT DEFAULT 'PENDING'
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS indexed_folders (
            folder_name TEXT PRIMARY KEY,
            scanned_at TIMESTAMP
        )
    """)

    # Schema migration
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(queue)")
    existing_cols = {row[1] for row in cursor.fetchall()}

    columns_to_add = [
        ("error_message", "TEXT"),
        ("worker_id", "INTEGER"),
        ("retry_count", "INTEGER DEFAULT 0"),
        ("updated_at", "TIMESTAMP"),
    ]
    for col_name, col_type in columns_to_add:
        if col_name not in existing_cols:
            conn.execute(f"ALTER TABLE queue ADD COLUMN {col_name} {col_type}")

    conn.execute("UPDATE queue SET status = 'PENDING', worker_id = NULL WHERE status = 'STAGED'")
    conn.commit()
    return conn

def populate_database(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT folder_name FROM indexed_folders")
    already_indexed = {row[0] for row in cursor.fetchall()}

    folders_to_scan = [f for f in TARGET_FOLDERS if f not in already_indexed]

    if not folders_to_scan:
        cursor.execute("SELECT COUNT(*) FROM queue WHERE status = 'COMPLETED'")
        completed = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM queue WHERE status = 'FAILED'")
        failed = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM queue WHERE status = 'PENDING'")
        pending = cursor.fetchone()[0]
        print(f"[*] All target folders indexed: {completed} completed, {failed} failed, {pending} pending.")
        return

    # Load all existing filenames and sizes from the database for cross-folder deduplication
    cursor.execute("SELECT full_path, size_bytes FROM queue")
    existing_fingerprints = {
        (Path(row[0]).name, row[1]) for row in cursor.fetchall()
    }
    print(f"[*] Loaded {len(existing_fingerprints)} existing file signatures for deduplication.")

    print(f"[*] Indexing {len(folders_to_scan)} remaining folder(s) from external HDD...")

    for folder_name in folders_to_scan:
        folder_path = SOURCE_ROOT / folder_name
        if not folder_path.exists():
            print(f"[!] Folder '{folder_name}' not found on external HDD. Skipping.")
            continue

        print(f"    Scanning: {folder_name}...")
        records = []
        skipped_duplicates = 0

        for root, _, files in os.walk(folder_path):
            for file in files:
                if file.startswith("._") or file.startswith("."):
                    continue
                if file.endswith(".json"):
                    continue

                stem = Path(file).stem.lower()
                if stem.endswith("-edited") or stem.endswith(" - edited") or stem.endswith("_edited"):
                    continue

                ext = Path(file).suffix.lower()
                if ext in ALL_SUPPORTED_EXTS:
                    full_p = Path(root) / file
                    try:
                        size = full_p.stat().st_size
                        
                        # Deduplication check against already uploaded/indexed items
                        file_signature = (file, size)
                        if file_signature in existing_fingerprints:
                            skipped_duplicates += 1
                            continue

                        rel_p = str(full_p.relative_to(SOURCE_ROOT))
                        now = datetime.now(timezone.utc).isoformat()
                        records.append((rel_p, str(full_p), size, "PENDING", None, None, 0, now))
                        
                        # Add newly found unique file to fingerprint set so internal duplicates within the folder are also caught
                        existing_fingerprints.add(file_signature)
                    except OSError:
                        continue

        if records:
            cursor.executemany(
                """INSERT OR IGNORE INTO queue 
                   (rel_path, full_path, size_bytes, status, error_message, worker_id, retry_count, updated_at) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                records
            )

        now_str = datetime.now(timezone.utc).isoformat()
        cursor.execute("INSERT OR REPLACE INTO indexed_folders VALUES (?, ?)", (folder_name, now_str))
        conn.commit()
        print(f"    Indexed {len(records)} unique items in '{folder_name}' (skipped {skipped_duplicates} duplicates).")


# ==============================================================================
# METADATA SIDECAR RESOLUTION
# ==============================================================================
def find_metadata_json(media_path: Path) -> Path | None:
    parent = media_path.parent
    
    # Fast bypass: if folder has no JSON files at all, exit immediately
    if not folder_has_json(parent):
        return None

    name = media_path.name
    stem = media_path.stem
    ext = media_path.suffix.lower()

    for cand in [
        parent / f"{name}.supplemental-metadata.json",
        parent / f"{name}.json",
        parent / f"{stem}.supplemental-metadata.json",
        parent / f"{stem}.json",
    ]:
        if cand.is_file() and not cand.name.startswith("._"):
            return cand

    num_match = re.search(r"^(.*?)(?:\s*\((\d+)\))(\.[^.]+)$", name)
    if num_match:
        base, num, orig_ext = num_match.groups()
        candidates = [
            parent / f"{base}{orig_ext}.supplemental-metadata({num}).json",
            parent / f"{base}{orig_ext}({num}).json",
            parent / f"{base}{orig_ext}.supplemental-metadata ({num}).json",
            parent / f"{base}{orig_ext} ({num}).json",
            parent / f"{base}({num}){orig_ext}.supplemental-metadata.json",
            parent / f"{base}({num}){orig_ext}.json",
            parent / f"{base}({num}).json",
            parent / f"{base}.supplemental-metadata({num}).json",
        ]
        for cand in candidates:
            if cand.is_file() and not cand.name.startswith("._"):
                return cand

    incompatible = (PHOTO_EXTS if ext in VIDEO_EXTS else VIDEO_EXTS) | ({".heic"} if ext != ".heic" else set())
    incompatible.discard(ext)

    best_candidate = None
    highest_score = -1

    try:
        json_files = [p for p in parent.glob("*.json") if not p.name.startswith("._")]
    except OSError:
        return None

    for json_file in json_files:
        jname = json_file.name.lower()
        if any(bad in jname for bad in incompatible):
            continue

        prefix_len = 0
        min_len = min(len(name), len(json_file.name))
        for i in range(min_len):
            if name[i].lower() == json_file.name[i].lower():
                prefix_len += 1
            else:
                break

        if prefix_len >= 25:
            score = prefix_len
            if ext in jname:
                score += 50
            if "supplemental-metadata" in jname:
                score += 20
            if score > highest_score:
                highest_score = score
                best_candidate = json_file

    return best_candidate

def process_file_metadata(args):
    local_path, hdd_path_str = args
    json_path = find_metadata_json(Path(hdd_path_str))
    if not json_path or not json_path.exists():
        return True, None

    try:
        with open(json_path, "r", encoding="utf-8", errors="ignore") as f:
            meta = json.load(f)

        cmd = ["exiftool", "-overwrite_original", "-preserve"]

        taken_time = meta.get("photoTakenTime", {}).get("timestamp")
        if taken_time:
            dt = datetime.fromtimestamp(int(taken_time), tz=timezone.utc)
            formatted = dt.strftime("%Y:%m:%d %H:%M:%S")
            cmd.extend([
                f"-AllDates={formatted}",
                f"-QuickTime:CreateDate={formatted}",
                f"-QuickTime:ModifyDate={formatted}",
                "-api", "QuickTimeUTC=1"
            ])

        geo = meta.get("geoData", {})
        lat = geo.get("latitude", 0.0)
        lon = geo.get("longitude", 0.0)
        alt = geo.get("altitude", 0.0)

        if lat != 0.0 or lon != 0.0:
            cmd.extend([
                f"-GPSLatitude={abs(lat)}",
                f"-GPSLatitudeRef={'N' if lat >= 0 else 'S'}",
                f"-GPSLongitude={abs(lon)}",
                f"-GPSLongitudeRef={'E' if lon >= 0 else 'W'}",
                f"-GPSAltitude={alt}",
            ])

        desc = meta.get("description", "").strip()
        if desc:
            cmd.extend([f"-ImageDescription={desc}", f"-Caption-Abstract={desc}"])

        cmd.append(str(local_path))
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        return True, None
    except Exception as e:
        return False, str(e)

# ==============================================================================
# BROWSER UPLOAD ENGINE (CDP + File Chooser)
# ==============================================================================
class NetworkUploadTracker:
    def __init__(self, page):
        self.page = page
        self.active_requests = 0
        self._lock = asyncio.Lock()
        self.page.on("request", self._handle_request)
        self.page.on("requestfinished", self._handle_done)
        self.page.on("requestfailed", self._handle_done)

    def _is_upload(self, url: str) -> bool:
        return any(k in url for k in ["/upload/", "upload.video.google.com", "/_/upload", "batchexecute"])

    async def _handle_request(self, request):
        if request.method == "POST" and self._is_upload(request.url):
            async with self._lock:
                self.active_requests += 1

    async def _handle_done(self, request):
        if request.method == "POST" and self._is_upload(request.url):
            async with self._lock:
                self.active_requests = max(0, self.active_requests - 1)
                
# ==============================================================================
# BROWSER UPLOAD ENGINE (HARDENED)
# ==============================================================================
async def upload_files_to_page(page, tracker, file_paths):
    """Triggers upload via '+' menu with DOM click recovery and network tracking."""
    if "photos.google.com" not in page.url:
        await page.goto("https://photos.google.com", wait_until="domcontentloaded")
        await asyncio.sleep(2)

    total_bytes = sum(p.stat().st_size for p in file_paths if p.exists())
    
    # Realistic timeout: 15 mins base + 1200 seconds (~20 mins) per GB
    # For a 3.5 GB video, this grants ~85 minutes instead of 34 minutes
    timeout_seconds = max(900, int(900 + (total_bytes / (1024 * 1024 * 1024)) * 1200))

    file_chooser = None
    for attempt in range(3):
        try:
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.5)

            plus_btn = page.locator('button[aria-label="Create and add photos"]').first
            await plus_btn.wait_for(state="visible", timeout=10000)
            await plus_btn.click()

            import_opt = page.locator(
                '[role="menuitem"][aria-label="Import photos from your computer"], '
                '[role="menuitem"]:has-text("Import photos")'
            ).first
            await import_opt.wait_for(state="visible", timeout=7000)

            async with page.expect_file_chooser(timeout=10000) as fc_info:
                await import_opt.click()
            file_chooser = await fc_info.value
            break
        except Exception:
            if attempt == 2:
                raise
            await asyncio.sleep(1.5)

    input_elem = file_chooser.element

    # Attach directly via Chrome CDP
    await input_elem.evaluate("el => el.setAttribute('data-gp-active-upload', '1')")
    cdp = await page.context.new_cdp_session(page)
    try:
        await cdp.send("DOM.enable")
        eval_res = await cdp.send("Runtime.evaluate", {
            "expression": "document.querySelector('input[data-gp-active-upload=\"1\"]')"
        })
        object_id = eval_res.get("result", {}).get("objectId")
        if not object_id:
            raise RuntimeError("Failed to resolve file input in CDP session.")

        node_info = await cdp.send("DOM.describeNode", {"objectId": object_id})
        backend_node_id = node_info.get("node", {}).get("backendNodeId")

        resolved_paths = [str(Path(p).resolve()) for p in file_paths]
        await cdp.send("DOM.setFileInputFiles", {
            "backendNodeId": backend_node_id,
            "files": resolved_paths
        })
    finally:
        try:
            await input_elem.evaluate("el => el.removeAttribute('data-gp-active-upload')")
        except Exception:
            pass
        await cdp.detach()

    # Dismiss quality modal
    await asyncio.sleep(2)
    continue_btn = page.locator(
        'div[role="dialog"] button:has-text("Continue"), '
        'div[role="dialog"] button:has-text("Upload")'
    ).first
    try:
        if await continue_btn.is_visible():
            await continue_btn.click()
    except Exception:
        pass

    done_locator = page.locator(
        'button:has-text("Add to album"), '
        'div.wgq1ae:has-text("backed up"), '
        'div[role="status"]:has-text("backed up"), '
        'div[role="status"]:has-text("Upload complete"), '
        'div[role="status"]:has-text("No items uploaded")'
    )
    error_dialog = page.locator('div[role="dialog"]:has-text("Can\'t upload")')
    
    start_time = asyncio.get_event_loop().time()
    failed_files = set()
    last_ping = start_time

    while True:
        now = asyncio.get_event_loop().time()
        elapsed = now - start_time
        if elapsed > timeout_seconds:
            raise TimeoutError(f"Upload stalled after {int(elapsed)} seconds.")

        # Keep tab active: send a gentle no-op mouse move every 60 seconds
        if now - last_ping > 60:
            try:
                await page.mouse.move(10, 10)
            except Exception:
                pass
            last_ping = now

        # Handle errors
        if await error_dialog.count() > 0 and await error_dialog.first.is_visible():
            dialog_text = await error_dialog.first.inner_text()
            for p in file_paths:
                if p.name in dialog_text:
                    failed_files.add(p.name)

            close_btn = error_dialog.locator('button:has-text("Done"), button:has-text("Got it")').first
            if await close_btn.is_visible():
                await close_btn.click()

            if not failed_files:
                return False, [p.name for p in file_paths]

        # Completion verified
        if await done_locator.count() > 0 and tracker.active_requests == 0:
            print("[✓] Batch upload confirmed by browser.")
            break

        await asyncio.sleep(3)

    card_close_btn = page.locator('div.zPNfib button[aria-label="Close"], button[jsname="plIjzf"]').first
    try:
        if await card_close_btn.is_visible():
            await card_close_btn.click()
    except Exception:
        pass

    return True, list(failed_files)

# ==============================================================================
# WORKER PIPELINE (RESILIENT)
# ==============================================================================
async def worker_loop(worker_id: int, page, tracker, db_lock, large_file_lock):
    worker_scratch = SCRATCH_BASE / f"worker_{worker_id}"
    worker_scratch.mkdir(parents=True, exist_ok=True)
    print(f"[*] Worker {worker_id} initialized.")

    batches_processed = 0

    while True:
        batch = []
        async with db_lock:
            conn = sqlite3.connect(DB_PATH, timeout=60.0)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT rel_path, full_path, size_bytes FROM queue 
                WHERE status = 'PENDING' AND retry_count < 3
                ORDER BY size_bytes ASC 
                LIMIT 150
            """)
            rows = cursor.fetchall()
            if not rows:
                conn.close()
                break

            current_size = 0
            for r_path, f_path, sz in rows:
                if current_size + sz > BATCH_SIZE_BYTES and len(batch) > 0:
                    break
                batch.append((r_path, f_path))
                current_size += sz

            cursor.executemany(
                "UPDATE queue SET status = 'STAGED', worker_id = ? WHERE rel_path = ?",
                [(worker_id, rp) for rp, _ in batch]
            )
            conn.commit()
            conn.close()

        mb = current_size / (1024 * 1024)
        print(f"[*] Worker {worker_id}: Staged {len(batch)} items ({mb:.1f} MB)")

        local_paths = []
        work_items = []
        for rel_path, hdd_path_str in batch:
            hdd_p = Path(hdd_path_str)
            local_p = worker_scratch / hdd_p.name
            if local_p.exists():
                local_p = worker_scratch / f"{Path(rel_path).parent.name}_{hdd_p.name}"
            shutil.copy2(hdd_p, local_p)
            local_paths.append(local_p)
            work_items.append((local_p, hdd_path_str))

        with ProcessPoolExecutor(max_workers=EXIF_WORKERS) as executor:
            executor.map(process_file_metadata, work_items)

        # Dedicated bandwidth lock for any upload >= 1.5 GB to prevent network stalls
        is_heavy_batch = any(p.stat().st_size >= int(1.5 * 1024 * 1024 * 1024) for p in local_paths if p.exists())

        try:
            if is_heavy_batch:
                print(f"[*] Worker {worker_id}: Heavy file detected ({mb:.1f} MB). Acquiring exclusive bandwidth lock...")
                async with large_file_lock:
                    success, failed_names = await upload_files_to_page(page, tracker, local_paths)
            else:
                success, failed_names = await upload_files_to_page(page, tracker, local_paths)

            # Commit to SQLite IMMEDIATELY on confirmation
            async with db_lock:
                conn = sqlite3.connect(DB_PATH, timeout=60.0)
                now = datetime.now(timezone.utc).isoformat()

                if success and not failed_names:
                    conn.executemany(
                        "UPDATE queue SET status = 'COMPLETED', updated_at = ? WHERE rel_path = ?",
                        [(now, rp) for rp, _ in batch]
                    )
                    print(f"[✓] Worker {worker_id}: Committed {len(batch)} items.")
                else:
                    for rp, fp in batch:
                        filename = Path(fp).name
                        if filename in failed_names:
                            conn.execute(
                                "UPDATE queue SET status = 'FAILED', error_message = 'Google Photos rejected file', updated_at = ? WHERE rel_path = ?",
                                (now, rp)
                            )
                        else:
                            conn.execute(
                                "UPDATE queue SET status = 'COMPLETED', updated_at = ? WHERE rel_path = ?",
                                (now, rp)
                            )
                    print(f"[!] Worker {worker_id}: Batch handled with {len(failed_names)} isolated failures.")

                conn.commit()
                conn.close()

            batches_processed += 1
            # Periodic gentle reload only every 20 batches (every ~3000 items)
            if batches_processed % 20 == 0:
                try:
                    await page.reload(wait_until="domcontentloaded", timeout=45000)
                except Exception:
                    pass

        except Exception as e:
            print(f"[!] Worker {worker_id} upload failed: {e}. Resetting batch with retry counter.")
            async with db_lock:
                conn = sqlite3.connect(DB_PATH, timeout=60.0)
                conn.executemany(
                    "UPDATE queue SET status = 'PENDING', retry_count = retry_count + 1, worker_id = NULL WHERE rel_path = ?",
                    [(rp,) for rp, _ in batch]
                )
                conn.commit()
                conn.close()

            # Safe recovery reload wrapped in try-except so it can never crash the script
            try:
                await page.keyboard.press("Escape")
                await asyncio.sleep(2)
                await page.reload(wait_until="domcontentloaded", timeout=45000)
            except Exception:
                pass

        finally:
            for p in worker_scratch.glob("*"):
                try:
                    p.unlink()
                except OSError:
                    pass

    print(f"[*] Worker {worker_id} finished all queued work.")
    
# ==============================================================================
# MAIN
# ==============================================================================
async def main():
    if SCRATCH_BASE.exists():
        shutil.rmtree(SCRATCH_BASE)
    SCRATCH_BASE.mkdir(parents=True, exist_ok=True)

    conn = init_db()
    populate_database(conn)
    conn.close()

    db_lock = asyncio.Lock()
    large_file_lock = asyncio.Lock()

    print("[*] Connecting to Chrome on 127.0.0.1:9222...")
    async with async_playwright() as p:
        try:
            browser = await p.chromium.connect_over_cdp("http://127.0.0.1:9222")
        except Exception:
            print("\n[!] Connection failed. Launch Chrome using:")
            print('/Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome --remote-debugging-port=9222 --user-data-dir="$HOME/gp_chrome_profile" &\n')
            return

        context = browser.contexts[0]
        pages = []
        trackers = []
        for i in range(CONCURRENT_UPLOAD_WORKERS):
            page = await context.new_page()
            await page.goto("https://photos.google.com", wait_until="domcontentloaded")
            pages.append(page)
            trackers.append(NetworkUploadTracker(page))

        tasks = [
            asyncio.create_task(worker_loop(i, pages[i], trackers[i], db_lock, large_file_lock))
            for i in range(CONCURRENT_UPLOAD_WORKERS)
        ]

        await asyncio.gather(*tasks)
        print("\n[🎉] Complete library processed. Inspect gp_pipeline.db for summary.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[*] Script interrupted by user.")