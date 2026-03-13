# -*- coding: utf-8 -*-
import os
import platform
import time
import asyncio
import aiofiles   # pip install aiofiles
import httpx
import uvicorn
import threading
from datetime import datetime
from fastapi import FastAPI

app = FastAPI()

MY_APP_PORT = 8520
ORTHANC_BASE = "http://localhost:8042"
ORTHANC_AUTH = ("admin", "password")
SOURCE_PATH = r"G:\A"
# SOURCE_PATH = r"G:\A"
# SOURCE_PATH = r"C:\Users\Subin-PC\Downloads\DIcom_test_data\RADON_TEST_DATA\cd_test_data"

# CHANGE 1: Raise this dramatically — Orthanc is local, no network RTT
# 8 was conservative for a real network. For localhost, 32–64 is safe.
MAX_CONCURRENT_UPLOADS = 48
semaphore = asyncio.Semaphore(MAX_CONCURRENT_UPLOADS)

progress_counter = 0
IGNORED_EXTS = ('.exe', '.inf', '.htm', '.html', '.jar', '.txt', '.xml', '.bmp', '.png', '.ico')

def get_ts():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


# ---------------------------------------------------------------------------
# CD / removable drive detection
# ---------------------------------------------------------------------------

def detect_cd_drives() -> list:
    system, drives = platform.system(), []
    if system == "Windows":
        import ctypes
        bitmask = ctypes.windll.kernel32.GetLogicalDrives()
        for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
            if bitmask & 1:
                path = f"{letter}:\\"
                if ctypes.windll.kernel32.GetDriveTypeW(path) in (5, 2):
                    drives.append(path)
            bitmask >>= 1
    elif system == "Linux":
        for base in ("/media", "/mnt", "/run/media"):
            if os.path.isdir(base):
                for entry in os.scandir(base):
                    if entry.is_dir():
                        drives.append(entry.path)
                        try:
                            for sub in os.scandir(entry.path):
                                if sub.is_dir():
                                    drives.append(sub.path)
                        except PermissionError:
                            pass
        try:
            with open("/proc/mounts") as f:
                for line in f:
                    parts = line.split()
                    if len(parts) >= 2 and parts[0].startswith("/dev/sr"):
                        if parts[1] not in drives:
                            drives.append(parts[1])
        except Exception:
            pass
    elif system == "Darwin":
        if os.path.isdir("/Volumes"):
            for entry in os.scandir("/Volumes"):
                if entry.is_dir():
                    drives.append(entry.path)
    return drives


def prompt_source_path() -> str:
    """
    Detect removable / optical drives and let the user pick or type a path.
    Returns the chosen SOURCE_PATH string.
    """
    print("\n" + "=" * 60)
    print("  DICOM Importer — Select Source Path")
    print("=" * 60)

    drives = detect_cd_drives()

    if drives:
        print("\nDetected removable / optical drives:")
        for idx, d in enumerate(drives, start=1):
            print(f"  [{idx}] {d}")
    else:
        print("\n[INFO] No removable drives detected.")

    print(f"  [0] Enter a custom path manually")
    print()

    while True:
        try:
            raw = input("Enter choice number (or 0 for custom path): ").strip()
            choice = int(raw)
            if choice == 0:
                break
            if 1 <= choice <= len(drives):
                chosen = drives[choice - 1]
                print(f"\n[SOURCE] Using: {chosen}\n")
                return chosen
        except (ValueError, EOFError):
            pass
        if drives:
            print(f"Please enter a number between 0 and {len(drives)}.")
        else:
            print("Please enter 0 to type a custom path.")

    # Manual entry
    while True:
        try:
            path = input("Enter the full source path: ").strip()
        except EOFError:
            path = ""
        if path and os.path.isdir(path):
            print(f"\n[SOURCE] Using: {path}\n")
            return path
        print(f"[ERROR] '{path}' is not a valid directory. Please try again.")

async def upload_single_file(client, file_path, total_files):
    global progress_counter

    if file_path.lower().endswith(IGNORED_EXTS):
        return None

    async with semaphore:
        try:
            # CHANGE 2: Use aiofiles for non-blocking reads.
            # Previously, open() was synchronous — it blocked the entire event loop
            # while the CD drive spun up. With aiofiles, the loop stays free to
            # fire off other uploads while one file is being read from disk.
            async with aiofiles.open(file_path, "rb") as f:
                content = await f.read()
            
            if not content:
                return None

            response = await client.post(
                f"{ORTHANC_BASE}/instances",
                content=content,
                timeout=60.0
            )

            progress_counter += 1
            if progress_counter % 100 == 0 or progress_counter == total_files:
                percent = (progress_counter / total_files) * 100
                print(f"[{get_ts()}] [PROGRESS] {progress_counter}/{total_files} ({percent:.1f}%)")

            if response.status_code == 200:
                return response.json().get("ParentStudy")
            return None

        except Exception:
            return None


# ---------------------------------------------------------------------------
# DICOM Viewer — C-STORE via Orthanc modalities
# ---------------------------------------------------------------------------
# Viewers (RadiAnt, OsiriX, Horos, etc.) register themselves as DICOM
# modalities inside Orthanc. After upload, we query GET /modalities to get
# the list, then POST /modalities/{name}/store to push studies to the viewer
# over C-STORE — the same mechanism the frontend used.

async def _list_modalities(client: httpx.AsyncClient) -> list:
    """Return list of modality names configured in Orthanc."""
    try:
        r = await client.get(f"{ORTHANC_BASE}/modalities")
        if r.status_code == 200:
            return r.json()          # e.g. ["RADIANT", "OSIRIX"]
        print(f"[CSTORE] GET /modalities failed: HTTP {r.status_code}")
    except Exception as exc:
        print(f"[CSTORE] GET /modalities error: {exc}")
    return []


async def _cstore_to_modality(client: httpx.AsyncClient,
                               modality: str,
                               orthanc_study_ids: list) -> bool:
    """
    Push a list of Orthanc study IDs to a DICOM modality via C-STORE.
    Orthanc handles the DICOM network transfer internally.
    Returns True on success.
    """
    print(f"[CSTORE] Sending {len(orthanc_study_ids)} study(ies) → '{modality}' ...")
    try:
        r = await client.post(
            f"{ORTHANC_BASE}/modalities/{modality}/store",
            json=orthanc_study_ids,
            timeout=300,
        )
        if r.status_code == 200:
            print(f"[CSTORE] '{modality}' — C-STORE succeeded.")
            return True
        print(f"[CSTORE] '{modality}' — C-STORE failed: HTTP {r.status_code}  {r.text[:200]}")
    except Exception as exc:
        print(f"[CSTORE] '{modality}' — C-STORE error: {exc}")
    return False


def _prompt_modality(modalities: list) -> int:
    """
    Synchronous terminal prompt — runs in a thread executor so the event
    loop is never blocked waiting for keyboard input.
    Returns 0-based index of chosen modality, or -1 to skip.
    """
    print(f"\n[CSTORE] Multiple DICOM modalities registered in Orthanc.")
    print(f"[CSTORE] Choose which viewer to send studies to:")
    for idx, name in enumerate(modalities, start=1):
        print(f"  [{idx}] {name}")
    print(f"  [0] Skip — do not send to any viewer")

    while True:
        try:
            raw = input(f"[CSTORE] Enter choice (0–{len(modalities)}): ").strip()
            choice = int(raw)
            if 0 <= choice <= len(modalities):
                return choice - 1   # -1 when user enters 0
        except (ValueError, EOFError):
            pass
        print(f"[CSTORE] Invalid input — enter a number between 0 and {len(modalities)}.")


async def send_to_viewer(orthanc_study_ids: list) -> None:
    """
    Query Orthanc for registered modalities (DICOM viewers) and C-STORE
    the uploaded studies to one of them.

      0 modalities → prints a message and returns.
      1 modality   → sends automatically, no prompt.
      2+ modalities → prompts user in terminal to pick one.
    """
    if not orthanc_study_ids:
        print("[CSTORE] No Orthanc study IDs collected — skipping C-STORE.")
        return

    async with httpx.AsyncClient(auth=ORTHANC_AUTH, http2=True, timeout=30) as client:
        modalities = await _list_modalities(client)

    print(f"[CSTORE] Orthanc modalities found: {modalities if modalities else 'none'}")

    if not modalities:
        print("[CSTORE] No modalities registered in Orthanc.")
        print("[CSTORE] Register your DICOM viewer (RadiAnt, OsiriX, etc.) in")
        print("[CSTORE]   Orthanc → Configuration → DicomModalities, then retry.")
        return

    # Choose modality
    if len(modalities) == 1:
        chosen = modalities[0]
        print(f"[CSTORE] One modality found — auto-sending to '{chosen}' ...")
    else:
        loop   = asyncio.get_event_loop()
        idx    = await loop.run_in_executor(None, _prompt_modality, modalities)
        if idx < 0:
            print("[CSTORE] Skipped — studies not forwarded to any viewer.")
            return
        chosen = modalities[idx]

    # Send
    async with httpx.AsyncClient(auth=ORTHANC_AUTH, http2=True, timeout=300) as client:
        await _cstore_to_modality(client, chosen, orthanc_study_ids)


@app.post("/fast-import")
async def fast_import():
    global progress_counter
    progress_counter = 0
    t_start = time.monotonic()

    print(f"[{get_ts()}] [SERVER] Scanning {SOURCE_PATH}...")
    files_to_process = [
        os.path.join(root, f)
        for root, _, filenames in os.walk(SOURCE_PATH)
        for f in filenames
    ]
    total_files = len(files_to_process)
    print(f"[{get_ts()}] [SERVER] Found {total_files} files. Starting import...")

    # CHANGE 3: Enable HTTP/2. This lets httpx multiplex all requests over a
    # single connection instead of opening 48 separate TCP connections to
    # Orthanc. Drastically reduces connection-setup overhead.
    # Requires: pip install httpx[http2]  (installs h2 library)
    async with httpx.AsyncClient(
        auth=ORTHANC_AUTH,
        timeout=None,
        http2=True,                          # <-- HTTP/2 multiplexing
        limits=httpx.Limits(
            max_connections=MAX_CONCURRENT_UPLOADS,
            max_keepalive_connections=MAX_CONCURRENT_UPLOADS,
        )
    ) as client:
        tasks = [upload_single_file(client, f, total_files) for f in files_to_process]
        results = await asyncio.gather(*tasks)

    captured = [i for i in results if i is not None]
    duration = time.monotonic() - t_start

    print(f"[{get_ts()}] [COMPLETE] {duration:.2f}s — {len(captured)} instances indexed.")

    # Collect unique Orthanc study IDs from uploaded instances
    orthanc_study_ids = list({r for r in captured if r})
    print(f"[CSTORE] Collected {len(orthanc_study_ids)} unique Orthanc study ID(s).")
    await send_to_viewer(orthanc_study_ids)

    return {
        "status": "completed",
        "total_scanned": total_files,
        "successfully_indexed": len(captured),
        "total_time_sec": round(duration, 2),
    }


def start_api():
    uvicorn.run(app, host="127.0.0.1", port=MY_APP_PORT, log_level="error")

async def client_trigger():
    await asyncio.sleep(2)
    print(f"[{get_ts()}] [CLIENT] Triggering import for {SOURCE_PATH}...")
    async with httpx.AsyncClient(timeout=None, http2=True) as client:
        try:
            r = await client.post(f"http://127.0.0.1:{MY_APP_PORT}/fast-import")
            print(f"\n[{get_ts()}] RESULT: {r.json()}")
        except Exception as e:
            print(f"\n[{get_ts()}] [CLIENT ERROR] {e}")

if __name__ == "__main__":
    SOURCE_PATH = prompt_source_path()
    server_thread = threading.Thread(target=start_api, daemon=True)
    server_thread.start()
    try:
        asyncio.run(client_trigger())
    except KeyboardInterrupt:
        print("\nExiting...")