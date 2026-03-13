# -*- coding: utf-8 -*-
import os
import platform
import time
import asyncio
import webbrowser
import aiofiles   # pip install aiofiles
import httpx
import uvicorn
import threading
import pydicom                          # pip install pydicom
from pathlib import Path
from datetime import datetime
from typing import Optional, List
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI()

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"

MY_APP_PORT = 8000
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
progress_total = 0
IGNORED_EXTS = ('.exe', '.inf', '.htm', '.html', '.jar', '.txt', '.xml', '.bmp', '.png', '.ico')

# Thread pool size for fallback per-file header scanning
PRELOAD_WORKERS = 8

# Tags to read for fast per-file header scan (fallback path)
_DICOM_TAGS = [
    "PatientName", "PatientID", "PatientSex", "PatientAge",
    "StudyInstanceUID", "StudyDescription", "StudyDate", "StudyTime",
    "Modality", "SeriesNumber", "SeriesInstanceUID",
]

def get_ts():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def _norm_str(val) -> str:
    """Normalise a pydicom value to a plain Python string."""
    if val is None:
        return ""
    s = str(val).strip()
    return s.replace("\x00", "")


def _fmt_duration(seconds: float) -> str:
    """Human-friendly elapsed time string."""
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(seconds, 60)
    return f"{int(m)}m {s:.0f}s"


def _print_scan_timing(*, method: str, elapsed: float, file_count: int,
                       patient_count: int, study_count: int,
                       series_count: int = 0):
    """Pretty-print scan results to the console."""
    print(f"\n{'─' * 50}")
    print(f"  Scan method   : {method}")
    print(f"  Elapsed       : {_fmt_duration(elapsed)}")
    print(f"  Files / images: {file_count}")
    print(f"  Patients      : {patient_count}")
    print(f"  Studies       : {study_count}")
    if series_count:
        print(f"  Series        : {series_count}")
    print(f"{'─' * 50}\n")


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


# ---------------------------------------------------------------------------
# Step 2a — Fast path: parse DICOMDIR index (when available)
# ---------------------------------------------------------------------------

def _parse_dicomdir(dicomdir_path: str) -> dict:
    """
    Parse a DICOMDIR file and return the same dict structure as scan_drive().
    This is orders-of-magnitude faster than reading every DICOM file header.
    """
    dicomdir_path = os.path.abspath(dicomdir_path)
    dicomdir_dir  = os.path.dirname(dicomdir_path)
    t0 = time.monotonic()
    print(f"[DICOMDIR] Parsing: {dicomdir_path}")
    print(f"[DICOMDIR] File-set root: {dicomdir_dir}")

    dicomdir = pydicom.dcmread(dicomdir_path)
    if not hasattr(dicomdir, "DirectoryRecordSequence"):
        print("[DICOMDIR] No DirectoryRecordSequence — cannot parse.")
        return {}

    studies: dict = defaultdict(lambda: defaultdict(list))
    cur_patient = cur_patient_id = cur_patient_sex = cur_patient_age = ""
    cur_study_uid = cur_desc = cur_date = cur_study_time = ""
    cur_study_id = cur_accession = cur_modality = ""
    cur_series_desc = cur_series_num = cur_series_uid = ""
    cur_manufacturer = cur_model = cur_institution = ""
    image_count = patient_count = study_count = series_count = 0
    first_path_shown = False

    for record in dicomdir.DirectoryRecordSequence:
        rtype = _norm_str(getattr(record, "DirectoryRecordType", "")).upper()

        if rtype == "PATIENT":
            patient_count  += 1
            cur_patient_id  = _norm_str(getattr(record, "PatientID",  ""))
            cur_patient_sex = _norm_str(getattr(record, "PatientSex", ""))
            cur_patient_age = _norm_str(getattr(record, "PatientAge", ""))
            cur_patient     = _norm_str(getattr(record, "PatientName", "Unknown")) or "Unknown"

        elif rtype == "STUDY":
            study_count   += 1
            cur_study_uid  = _norm_str(getattr(record, "StudyInstanceUID", "Unknown")) or "Unknown"
            cur_desc       = _norm_str(getattr(record, "StudyDescription", ""))
            cur_date       = _norm_str(getattr(record, "StudyDate",        ""))
            cur_study_time = _norm_str(getattr(record, "StudyTime",        ""))
            cur_study_id   = _norm_str(getattr(record, "StudyID",          ""))
            cur_accession  = _norm_str(getattr(record, "AccessionNumber",  ""))

        elif rtype == "SERIES":
            series_count    += 1
            cur_modality     = _norm_str(getattr(record, "Modality",              ""))
            cur_series_desc  = _norm_str(getattr(record, "SeriesDescription",     ""))
            cur_series_num   = _norm_str(getattr(record, "SeriesNumber",          ""))
            cur_series_uid   = _norm_str(getattr(record, "SeriesInstanceUID",     ""))
            cur_manufacturer = _norm_str(getattr(record, "Manufacturer",          ""))
            cur_model        = _norm_str(getattr(record, "ManufacturerModelName", ""))
            cur_institution  = _norm_str(getattr(record, "InstitutionName",       ""))

        elif rtype == "IMAGE":
            ref_file_id = getattr(record, "ReferencedFileID", None)
            if ref_file_id is None:
                continue

            if hasattr(ref_file_id, "__iter__") and not isinstance(ref_file_id, str):
                parts = [str(p).strip() for p in ref_file_id if str(p).strip()]
            else:
                raw = str(ref_file_id).strip()
                if "\\" in raw:
                    parts = [p for p in raw.split("\\") if p]
                elif "/" in raw:
                    parts = [p for p in raw.split("/") if p]
                else:
                    parts = [raw]

            if not parts:
                continue

            file_path = os.path.join(dicomdir_dir, *parts)

            if not first_path_shown:
                first_path_shown = True
                exists = os.path.isfile(file_path)
                print(f"[DICOMDIR] Sample path: {file_path!r}  exists={exists}")
                if not exists:
                    print("[DICOMDIR] WARNING: file not found — check path construction!")
                    print(f"[DICOMDIR]   dicomdir_dir={dicomdir_dir!r}  parts={parts}")

            studies[cur_patient][cur_study_uid].append({
                "path":            file_path,
                "patient":         cur_patient,
                "patient_id":      cur_patient_id,
                "patient_sex":     cur_patient_sex,
                "patient_age":     cur_patient_age,
                "study_uid":       cur_study_uid,
                "desc":            cur_desc,
                "date":            cur_date,
                "study_time":      cur_study_time,
                "study_id":        cur_study_id,
                "accession":       cur_accession,
                "modality":        cur_modality,
                "series_desc":     cur_series_desc,
                "series_num":      cur_series_num,
                "series_uid":      cur_series_uid,
                "manufacturer":    cur_manufacturer,
                "model":           cur_model,
                "institution":     cur_institution,
                "slice_thickness": _norm_str(getattr(record, "SliceThickness", "")),
                "rows":            _norm_str(getattr(record, "Rows",           "")),
                "columns":         _norm_str(getattr(record, "Columns",        "")),
                "instance_number": _norm_str(getattr(record, "InstanceNumber", "")),
            })
            image_count += 1

    elapsed = time.monotonic() - t0
    print(
        f"[DICOMDIR] Parsed in {elapsed * 1000:.0f}ms — "
        f"{patient_count} patient(s), {study_count} study(ies), "
        f"{series_count} series, {image_count} images"
    )
    _print_scan_timing(
        method="DICOMDIR",
        elapsed=elapsed,
        file_count=image_count,
        patient_count=patient_count,
        study_count=study_count,
        series_count=series_count,
    )
    return {p: dict(s) for p, s in studies.items()}


# ---------------------------------------------------------------------------
# Step 2b — Fallback: parallel DICOM header scan (no DICOMDIR)
# ---------------------------------------------------------------------------

def _read_dicom_header(fpath: str) -> Optional[dict]:
    """Read only the tags we need — 3-5× faster than full header decode."""
    try:
        ds = pydicom.dcmread(fpath, specific_tags=_DICOM_TAGS)
        return {
            "path":            fpath,
            "patient":         _norm_str(getattr(ds, "PatientName",      "Unknown")) or "Unknown",
            "patient_id":      _norm_str(getattr(ds, "PatientID",        "")),
            "patient_sex":     _norm_str(getattr(ds, "PatientSex",       "")),
            "patient_age":     _norm_str(getattr(ds, "PatientAge",       "")),
            "study_uid":       _norm_str(getattr(ds, "StudyInstanceUID", "Unknown")) or "Unknown",
            "desc":            _norm_str(getattr(ds, "StudyDescription", "")),
            "date":            _norm_str(getattr(ds, "StudyDate",        "")),
            "study_time":      _norm_str(getattr(ds, "StudyTime",        "")),
            "study_id":        "",
            "accession":       "",
            "modality":        _norm_str(getattr(ds, "Modality",         "")),
            "series_desc":     "",
            "series_num":      _norm_str(getattr(ds, "SeriesNumber",     "")),
            "series_uid":      _norm_str(getattr(ds, "SeriesInstanceUID","")),
            "manufacturer":    "",
            "model":           "",
            "institution":     "",
            "slice_thickness": "",
            "rows":            "",
            "columns":         "",
            "instance_number": "",
        }
    except Exception:
        return None


def scan_drive(drive_path: str) -> dict:
    """
    Scan a directory for DICOM metadata.

    Fast path  → try DICOMDIR (milliseconds, no per-file I/O).
    Fallback   → parallel per-file header scan with PRELOAD_WORKERS threads.
    """
    # Fast path — DICOMDIR (case-insensitive check)
    for name in ("DICOMDIR", "dicomdir"):
        dicomdir_path = os.path.join(drive_path, name)
        if os.path.isfile(dicomdir_path):
            print("[SCAN] DICOMDIR found — using fast path")
            result = _parse_dicomdir(dicomdir_path)
            if result:
                return result
            print("[SCAN] DICOMDIR parse returned empty — falling back to full scan")
            break

    # Fallback — walk + parallel header scan
    print(f"[SCAN] Walking directory: {drive_path}")
    t0 = time.monotonic()
    all_paths = [
        os.path.join(root, fname)
        for root, _dirs, files in os.walk(drive_path)
        for fname in files
    ]
    print(f"[SCAN] Found {len(all_paths)} files. Scanning DICOM headers with "
          f"{PRELOAD_WORKERS} threads ...")

    studies: dict = defaultdict(lambda: defaultdict(list))
    scanned = valid = 0
    with ThreadPoolExecutor(max_workers=PRELOAD_WORKERS) as pool:
        for result in pool.map(_read_dicom_header, all_paths):
            scanned += 1
            if scanned % 200 == 0:
                print(f"[SCAN]   scanned {scanned}/{len(all_paths)} headers ...")
            if result:
                valid += 1
                studies[result["patient"]][result["study_uid"]].append(result)

    elapsed = time.monotonic() - t0
    print(f"[SCAN] Scan complete in {_fmt_duration(elapsed)}: "
          f"{valid} DICOM files, {len(studies)} patients, "
          f"{sum(len(s) for s in studies.values())} studies")
    _print_scan_timing(
        method="File Scan (fallback)",
        elapsed=elapsed,
        file_count=valid,
        patient_count=len(studies),
        study_count=sum(len(s) for s in studies.values()),
    )
    return {p: dict(s) for p, s in studies.items()}


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
    Log collected Orthanc study IDs. Actual viewer selection is now
    handled via the frontend through the /send-to-viewer endpoint.
    """
    if not orthanc_study_ids:
        print("[CSTORE] No Orthanc study IDs collected — skipping C-STORE.")
        return

    async with httpx.AsyncClient(auth=ORTHANC_AUTH, http2=True, timeout=30) as client:
        modalities = await _list_modalities(client)

    print(f"[CSTORE] Orthanc modalities found: {modalities if modalities else 'none'}")
    print(f"[CSTORE] {len(orthanc_study_ids)} study ID(s) ready for viewer selection via frontend.")


# ---------------------------------------------------------------------------
# API: Frontend + Drive detection + Progress + Scan
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def serve_index():
    """Serve the frontend."""
    html_file = TEMPLATES_DIR / "index.html"
    return HTMLResponse(content=html_file.read_text(encoding="utf-8"))


@app.get("/detect-drives")
async def api_detect_drives():
    """Return list of detected removable / optical drives."""
    drives = detect_cd_drives()
    return {"drives": drives}


@app.get("/modalities")
async def api_modalities():
    """Return list of DICOM modalities (viewers) registered in Orthanc."""
    async with httpx.AsyncClient(auth=ORTHANC_AUTH, http2=True, timeout=30) as client:
        modalities = await _list_modalities(client)
    return {"modalities": modalities}


@app.post("/send-to-viewer")
async def api_send_to_viewer(payload: dict):
    """
    Send Orthanc study IDs to a specific DICOM modality via C-STORE.
    Expects JSON: {"modality": "RADIANT", "study_ids": ["abc", "def"]}
    """
    modality = payload.get("modality", "").strip()
    study_ids = payload.get("study_ids", [])

    if not modality:
        return JSONResponse({"error": "No modality specified"}, status_code=400)
    if not study_ids:
        return JSONResponse({"error": "No study IDs provided"}, status_code=400)

    async with httpx.AsyncClient(auth=ORTHANC_AUTH, http2=True, timeout=300) as client:
        ok = await _cstore_to_modality(client, modality, study_ids)

    if ok:
        return {"status": "success", "modality": modality, "studies_sent": len(study_ids)}
    else:
        return JSONResponse(
            {"error": f"C-STORE to '{modality}' failed", "modality": modality},
            status_code=502,
        )


@app.get("/progress")
async def api_progress():
    """Return current upload progress for the frontend progress bar."""
    return {"done": progress_counter, "total": progress_total}

@app.get("/scan")
async def api_scan(path: str = Query(default=None, description="Override scan path")):
    """
    Scan SOURCE_PATH (or an overridden ?path=...) for DICOM studies.
    Returns a summary with patient/study counts and timing.
    """
    scan_path = path or SOURCE_PATH
    if not os.path.isdir(scan_path):
        return {"error": f"Path not found: {scan_path}"}

    loop = asyncio.get_event_loop()
    t0 = time.monotonic()
    result = await loop.run_in_executor(None, scan_drive, scan_path)
    elapsed = time.monotonic() - t0

    # Build summary with per-study file paths
    summary = []
    total_images = 0
    for patient, studies_dict in result.items():
        for study_uid, files in studies_dict.items():
            first = files[0] if files else {}
            img_count = len(files)
            total_images += img_count
            summary.append({
                "patient":     patient,
                "patient_id":  first.get("patient_id", ""),
                "patient_sex": first.get("patient_sex", ""),
                "patient_age": first.get("patient_age", ""),
                "study_uid":   study_uid,
                "description": first.get("desc", ""),
                "date":        first.get("date", ""),
                "modality":    first.get("modality", ""),
                "image_count": img_count,
                "files":       [f["path"] for f in files],
            })

    return {
        "status":       "ok",
        "scanned_path": scan_path,
        "patients":     len(result),
        "studies":      len(summary),
        "total_images": total_images,
        "elapsed_sec":  round(elapsed, 3),
        "studies_list": summary,
    }


@app.get("/scan-studies")
async def api_scan_studies(path: str = Query(default=None, description="Override scan path")):
    """
    Full study detail scan — returns the complete nested metadata dict.
    Heavier response; use /scan for a lighter summary.
    """
    scan_path = path or SOURCE_PATH
    if not os.path.isdir(scan_path):
        return {"error": f"Path not found: {scan_path}"}

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, scan_drive, scan_path)
    return {"status": "ok", "scanned_path": scan_path, "data": result}


@app.post("/upload-study")
async def upload_study(payload: dict):
    """
    Upload a single study's files to Orthanc.
    Expects JSON: {"files": ["path1", "path2", ...]}
    """
    global progress_counter, progress_total
    file_list = payload.get("files", [])
    if not file_list:
        return {"error": "No files provided"}

    progress_counter = 0
    progress_total = len(file_list)
    t_start = time.monotonic()

    print(f"[{get_ts()}] [UPLOAD-STUDY] Uploading {len(file_list)} file(s)...")

    async with httpx.AsyncClient(
        auth=ORTHANC_AUTH,
        timeout=None,
        http2=True,
        limits=httpx.Limits(
            max_connections=MAX_CONCURRENT_UPLOADS,
            max_keepalive_connections=MAX_CONCURRENT_UPLOADS,
        )
    ) as client:
        tasks = [upload_single_file(client, f, len(file_list)) for f in file_list]
        results = await asyncio.gather(*tasks)

    captured = [i for i in results if i is not None]
    duration = time.monotonic() - t_start

    print(f"[{get_ts()}] [UPLOAD-STUDY] Done in {duration:.2f}s — {len(captured)} indexed.")

    orthanc_study_ids = list({r for r in captured if r})
    if orthanc_study_ids:
        await send_to_viewer(orthanc_study_ids)

    return {
        "status": "completed",
        "total_scanned": len(file_list),
        "successfully_indexed": len(captured),
        "total_time_sec": round(duration, 2),
        "orthanc_study_ids": orthanc_study_ids,
    }


@app.post("/fast-import")
async def fast_import():
    global progress_counter, progress_total
    progress_counter = 0
    progress_total = 0
    t_start = time.monotonic()

    print(f"[{get_ts()}] [SERVER] Scanning {SOURCE_PATH}...")
    files_to_process = [
        os.path.join(root, f)
        for root, _, filenames in os.walk(SOURCE_PATH)
        for f in filenames
    ]
    total_files = len(files_to_process)
    progress_total = total_files
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
        "orthanc_study_ids": orthanc_study_ids,
    }


def start_api():
    uvicorn.run(app, host="0.0.0.0", port=MY_APP_PORT, log_level="error")

if __name__ == "__main__":
    SOURCE_PATH = prompt_source_path()

    server_thread = threading.Thread(target=start_api, daemon=True)
    server_thread.start()

    print(f"\n[SERVER] Running at  http://127.0.0.1:{MY_APP_PORT}")
    print(f"[SERVER] Opening browser...\n")
    webbrowser.open(f"http://127.0.0.1:{MY_APP_PORT}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nExiting...")
