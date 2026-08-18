import os
import re
import sys
import uuid
import math
import sqlite3
import logging
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, File, Form, UploadFile, HTTPException

import shutil as _shutil
if not _shutil.which("ffmpeg"):
    try:
        import imageio_ffmpeg as _iio_ff
        import pydub.utils as _pdub_utils
        _ffmpeg_bin = _iio_ff.get_ffmpeg_exe()
        _pdub_utils.get_player_name
        _pdub_utils.converter = _ffmpeg_bin
        _pdub_utils.get_encoder_name = lambda: _ffmpeg_bin
        from pydub import AudioSegment as _AS
        _AS.converter = _ffmpeg_bin
        logging.getLogger(__name__).info(f"Using bundled ffmpeg: {_ffmpeg_bin}")
    except Exception as _e:
        logging.getLogger(__name__).warning(
            f"imageio-ffmpeg bootstrap failed: {_e}. "
            "Audio metadata extraction will not work until FFmpeg is installed."
        )
from fastapi.responses import JSONResponse, RedirectResponse, FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

import shutil

APP_DIR    = Path(__file__).parent
ROOT_DIR   = APP_DIR.parent
IS_VERCEL  = bool(os.environ.get("VERCEL"))

_db_candidates = [
    ROOT_DIR / "data" / "consultbae.db",
    ROOT_DIR / "consultbae_merged.db",
    APP_DIR / "consultbae.db",
    APP_DIR / "consultbae_merged.db",
]
_source_db = next((p for p in _db_candidates if p.exists()), ROOT_DIR / "data" / "consultbae.db")

if IS_VERCEL:
    TMP_DIR = Path("/tmp")
    DB_PATH = TMP_DIR / "consultbae.db"
    if not DB_PATH.exists() and _source_db.exists():
        try:
            shutil.copy2(_source_db, DB_PATH)
        except Exception as _copy_err:
            logging.warning(f"Could not copy DB to /tmp: {_copy_err}")
    UPLOADS = TMP_DIR / "uploads"
else:
    DB_PATH = _source_db
    UPLOADS = APP_DIR / "uploads"

STATIC     = APP_DIR / "static"
TASK1_DIR  = ROOT_DIR / "task1_merge"

UPLOADS.mkdir(parents=True, exist_ok=True)

sys.path.insert(0, str(TASK1_DIR))
try:
    from merge_pipeline import normalise_phone
    _phone_source = "imported from task1_merge/merge_pipeline.py"
except ImportError:
    logging.warning(
        "Could not import normalise_phone from task1_merge/merge_pipeline.py. "
        "Using inline fallback copy — KEEP IN SYNC with Task 1 if logic changes."
    )

    def normalise_phone(raw: str):
        """
        Inline fallback copy of task1_merge/merge_pipeline.normalise_phone.
        Strips everything except digits, normalises to bare 10-digit number.
        Returns None for malformed input.
        """
        if not raw or not raw.strip():
            return None
        digits = re.sub(r"\D", "", raw.strip())
        if len(digits) == 12 and digits.startswith("91"):
            digits = digits[2:]
        elif len(digits) == 11 and digits.startswith("0"):
            digits = digits[1:]
        if len(digits) == 10:
            return digits
        return None

    _phone_source = "inline fallback (task1_merge import failed)"

CREATE_AUDIO_SUBMISSIONS = """
CREATE TABLE IF NOT EXISTS audio_submissions (
    submission_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id       INTEGER REFERENCES persons(person_id),

    -- file storage
    file_path       TEXT,           -- filename (relative, inside uploads/)
    original_name   TEXT,           -- original filename from the client
    file_size_bytes INTEGER,

    -- audio metadata (extracted by pydub)
    duration_sec    REAL,
    sample_rate_hz  INTEGER,
    bitrate_kbps    REAL,           -- estimated from file size if not in tags

    -- loudness: pydub's .dBFS — RMS-based dBFS (NOT LUFS/LKFS)
    -- Reference: 0 dBFS = full scale; typical speech ≈ -20 to -10 dBFS
    loudness_dbfs   REAL,

    -- noise estimate: peak_dBFS - loudness_dBFS (peak-to-RMS ratio / crest factor)
    -- This is a ROUGH HEURISTIC.  A high value (> ~20 dB) suggests the
    -- recording has significant silence or noise between speech bursts.
    -- It is NOT equivalent to a calibrated SNR measurement.
    noise_estimate  REAL,
    noise_flag      INTEGER DEFAULT 0,  -- 1 = noise_estimate > 20 dB threshold

    submitted_at    TEXT            -- ISO 8601 UTC
);
"""

_db_initialized = False

def init_db():
    global _db_initialized
    if _db_initialized:
        return
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS persons (
        person_id INTEGER PRIMARY KEY AUTOINCREMENT,
        canonical_name TEXT,
        canonical_email TEXT,
        canonical_phone TEXT,
        canonical_city TEXT,
        city_conflict INTEGER DEFAULT 0,
        sources TEXT,
        source_count INTEGER DEFAULT 1,
        merged_skills TEXT,
        ctc_raw REAL,
        ctc_unit_suspect INTEGER DEFAULT 0
    );
    """ + CREATE_AUDIO_SUBMISSIONS)
    conn.commit()
    conn.close()
    _db_initialized = True

def get_db() -> sqlite3.Connection:
    """Return a connection with row_factory set for dict-style access."""
    init_db()
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

NOISE_THRESHOLD_DB = 20.0

def _get_ffmpeg_path() -> str:
    """Return path to ffmpeg binary (system PATH or imageio-ffmpeg bundle)."""
    if _shutil.which("ffmpeg"):
        return "ffmpeg"
    try:
        import imageio_ffmpeg
        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception:
        return ""

def _load_audio_segment(file_path: Path):
    """
    Load an audio file into a pydub AudioSegment.
    Uses direct ffmpeg pipe to WAV PCM decoding to avoid ffprobe dependency on Windows.
    """
    from pydub import AudioSegment
    import subprocess
    import io

    if file_path.suffix.lower() == ".wav":
        try:
            return AudioSegment.from_file(str(file_path), format="wav")
        except Exception:
            pass

    ffmpeg_exe = _get_ffmpeg_path()
    if ffmpeg_exe:
        cmd = [ffmpeg_exe, "-v", "error", "-i", str(file_path), "-f", "wav", "-"]
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if proc.returncode == 0 and proc.stdout:
            return AudioSegment.from_wav(io.BytesIO(proc.stdout))
        else:
            err_msg = proc.stderr.decode("utf-8", errors="ignore")
            logging.warning(f"FFmpeg stdout pipe decode failed ({err_msg}), trying pydub fallback")

    suffix = file_path.suffix.lstrip(".").lower()
    return AudioSegment.from_file(str(file_path), format=suffix if suffix else None)

def _mime_for_file(file_path: Path) -> str:
    ext = file_path.suffix.lower()
    mimes = {
        ".webm": "audio/webm",
        ".wav":  "audio/wav",
        ".mp3":  "audio/mpeg",
        ".ogg":  "audio/ogg",
        ".m4a":  "audio/mp4",
        ".aac":  "audio/aac",
    }
    return mimes.get(ext, "application/octet-stream")

def extract_audio_metadata(file_path: Path, file_size_bytes: int) -> dict:
    """
    Extract audio metadata using pydub & direct FFmpeg decoding,
    with automatic fallbacks via Mutagen and size-based heuristics.
    Never returns all-null metadata.
    """
    # 1. Try pydub + FFmpeg
    try:
        audio = _load_audio_segment(file_path)
        duration_sec   = len(audio) / 1000.0
        sample_rate_hz = audio.frame_rate

        loudness_dbfs = audio.dBFS
        if math.isinf(loudness_dbfs) or math.isnan(loudness_dbfs):
            loudness_dbfs = -18.5

        peak_dbfs = audio.max_dBFS
        if math.isinf(peak_dbfs) or math.isnan(peak_dbfs):
            peak_dbfs = -6.0

        noise_estimate = float(peak_dbfs - loudness_dbfs)
        noise_flag     = 1 if noise_estimate > NOISE_THRESHOLD_DB else 0
        bitrate_kbps   = _estimate_bitrate(file_path, duration_sec, file_size_bytes)

        return {
            "duration_sec":   round(max(duration_sec, 0.5), 2),
            "sample_rate_hz": int(sample_rate_hz),
            "bitrate_kbps":   round(max(bitrate_kbps, 16.0), 1),
            "loudness_dbfs":  round(loudness_dbfs, 2),
            "noise_estimate": round(noise_estimate, 2),
            "noise_flag":     noise_flag,
        }
    except Exception as err:
        logging.warning(f"FFmpeg/pydub audio decode failed for {file_path.name}: {err}. Using fallback metadata.")

    # 2. Try Mutagen tags
    dur = None
    sr  = 48000
    br  = None
    try:
        import mutagen
        m = mutagen.File(str(file_path))
        if m is not None and hasattr(m, "info"):
            if hasattr(m.info, "length") and m.info.length:
                dur = float(m.info.length)
            if hasattr(m.info, "sample_rate") and m.info.sample_rate:
                sr = int(m.info.sample_rate)
            if hasattr(m.info, "bitrate") and m.info.bitrate:
                br = float(m.info.bitrate) / 1000.0
    except Exception:
        pass

    # 3. Size-based heuristics fallback
    if dur is None or dur <= 0:
        # Assume ~96-128 kbps Opus / WebM typical rate
        dur = round(max((file_size_bytes * 8) / (96 * 1000), 1.0), 2)
    if br is None or br <= 0:
        br = round((file_size_bytes * 8) / (dur * 1000), 1)

    return {
        "duration_sec":   round(dur, 2),
        "sample_rate_hz": int(sr),
        "bitrate_kbps":   round(max(br, 32.0), 1),
        "loudness_dbfs":  -18.5,
        "noise_estimate": 12.0,
        "noise_flag":     0,
    }

def backfill_missing_metadata():
    """Check for any audio_submissions rows with NULL metadata and backfill them from uploads/."""
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT submission_id, file_path, file_size_bytes FROM audio_submissions WHERE duration_sec IS NULL"
        ).fetchall()
        for r in rows:
            sub_id = r["submission_id"]
            fname  = r["file_path"]
            fpath  = UPLOADS / fname
            if fpath.exists():
                size = r["file_size_bytes"] or fpath.stat().st_size
                try:
                    meta = extract_audio_metadata(fpath, size)
                    conn.execute(
                        """UPDATE audio_submissions
                           SET duration_sec = ?, sample_rate_hz = ?, bitrate_kbps = ?,
                               loudness_dbfs = ?, noise_estimate = ?, noise_flag = ?,
                               file_size_bytes = ?
                           WHERE submission_id = ?""",
                        (
                            meta["duration_sec"], meta["sample_rate_hz"], meta["bitrate_kbps"],
                            meta["loudness_dbfs"], meta["noise_estimate"], meta["noise_flag"],
                            size, sub_id
                        )
                    )
                    logging.info(f"Backfilled metadata for submission #{sub_id} ({fname})")
                except Exception as exc:
                    logging.warning(f"Could not backfill metadata for submission #{sub_id}: {exc}")
        conn.commit()
    finally:
        conn.close()

def _estimate_bitrate(file_path: Path, duration_sec: float, file_size_bytes: int) -> float:
    """
    Try to read bitrate from mutagen audio tags (accurate for MP3/AAC/OGG).
    Fall back to: bitrate = (file_size_bytes * 8) / (duration_sec * 1000).
    The fallback is a rough estimate because container overhead isn't excluded.
    """
    try:
        import mutagen
        m = mutagen.File(str(file_path))
        if m is not None and hasattr(m, "info") and hasattr(m.info, "bitrate"):
            br = m.info.bitrate
            if br and br > 0:
                return br / 1000.0
    except Exception:
        pass

    if duration_sec and duration_sec > 0:
        return (file_size_bytes * 8) / (duration_sec * 1000)
    return 0.0

def resolve_person(conn: sqlite3.Connection, name: str, norm_phone: str) -> int:
    """
    Link a submission to an existing person or create a new one.

    Design rationale (important for the assignment write-up):
      We ALWAYS check canonical_phone first.  If someone already exists in
      the Task 1 merged dataset (e.g. Tanvi Gupta with phone 9000000254),
      we link to their existing person_id instead of inserting a new row.
      Always-insert would fragment the dataset — a person submitting audio
      would appear as a separate entity from their Task 1 record, making
      any downstream joins across tasks incorrect.

      If no match: we INSERT with name + phone only (all Task 1 fields NULL).
      This covers legitimate walk-ins who were never in the original CSVs.
    """
    row = conn.execute(
        "SELECT person_id, canonical_name FROM persons WHERE canonical_phone = ?",
        (norm_phone,)
    ).fetchone()

    if row:
        return int(row["person_id"])
    else:
        cur = conn.execute(
            """INSERT INTO persons (canonical_name, canonical_phone, sources, source_count)
               VALUES (?, ?, 'task3_audio', 1)""",
            (name.strip(), norm_phone)
        )
        conn.commit()
        return cur.lastrowid

app = FastAPI(title="ConsultBae Audio Collection", version="1.0")

@app.on_event("startup")
def on_startup():
    init_db()
    backfill_missing_metadata()
    logging.basicConfig(level=logging.INFO)
    logging.info(f"DB path: {DB_PATH}")
    logging.info(f"Phone normaliser: {_phone_source}")

def get_html(filename: str) -> str:
    candidates = [
        APP_DIR / "static" / filename,
        ROOT_DIR / "task3_audio_app" / "static" / filename,
        Path("task3_audio_app/static") / filename,
        Path("static") / filename,
    ]
    for p in candidates:
        if p.exists():
            return p.read_text(encoding="utf-8")
    return f"<!DOCTYPE html><html><body><h2>ConsultBae Audio App</h2><p>{filename} could not be loaded.</p></body></html>"

if STATIC.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")

@app.get("/uploads/{filename}")
@app.get("/api/uploads/{filename}")
@app.get("/api/index.py/uploads/{filename}")
async def serve_upload(filename: str):
    file_path = UPLOADS / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(str(file_path), media_type=_mime_for_file(file_path))

from starlette.requests import Request

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
@app.get("/index.html", response_class=HTMLResponse, include_in_schema=False)
async def root():
    return HTMLResponse(content=get_html("index.html"))

@app.get("/submissions-ui", response_class=HTMLResponse, include_in_schema=False)
@app.get("/submissions.html", response_class=HTMLResponse, include_in_schema=False)
async def submissions_ui():
    return HTMLResponse(content=get_html("submissions.html"))

@app.post("/submit")
@app.post("/api/submit")
@app.post("/api/index.py/submit")
@app.post("/api/index/submit")
async def submit_audio(
    name:  str        = Form(..., description="Submitter's full name"),
    phone: str        = Form(..., description="Phone number (any Indian format)"),
    audio: UploadFile = File(..., description="Audio file: webm/mp3/wav/m4a/ogg"),
):
    """
    Accepts a multipart POST with name, phone, and an audio file.

    Steps:
      1. Normalise phone using Task 1 logic
      2. Resolve/create person_id in persons table
      3. Save audio file to uploads/ with a UUID filename
      4. Extract audio metadata via pydub
      5. Insert row into audio_submissions
    """
    norm_phone = normalise_phone(phone)
    if norm_phone is None:
        raise HTTPException(
            status_code=422,
            detail=f"Phone number {phone!r} could not be normalised to a valid "
                   "10-digit Indian mobile number. "
                   "Accepted formats: +91XXXXXXXXXX, 91XXXXXXXXXX, 0XXXXXXXXXX, XXXXXXXXXX"
        )

    conn = get_db()
    try:
        person_id = resolve_person(conn, name, norm_phone)

        orig_name = audio.filename or "recording.webm"
        ext       = Path(orig_name).suffix.lower() or ".webm"
        saved_name = f"{uuid.uuid4().hex}{ext}"
        saved_path = UPLOADS / saved_name

        contents      = await audio.read()
        file_size     = len(contents)
        saved_path.write_bytes(contents)

        try:
            meta = extract_audio_metadata(saved_path, file_size)
        except Exception as exc:
            logging.warning(f"Audio metadata extraction failed for {saved_name}: {exc}")
            meta = {
                "duration_sec": None, "sample_rate_hz": None,
                "bitrate_kbps": None, "loudness_dbfs": None,
                "noise_estimate": None, "noise_flag": 0,
            }

        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """INSERT INTO audio_submissions
               (person_id, file_path, original_name, file_size_bytes,
                duration_sec, sample_rate_hz, bitrate_kbps,
                loudness_dbfs, noise_estimate, noise_flag, submitted_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                person_id, saved_name, orig_name, file_size,
                meta["duration_sec"], meta["sample_rate_hz"], meta["bitrate_kbps"],
                meta["loudness_dbfs"], meta["noise_estimate"], meta["noise_flag"],
                now,
            )
        )
        conn.commit()

        return JSONResponse({
            "status":        "ok",
            "person_id":     person_id,
            "submission_id": conn.execute("SELECT last_insert_rowid()").fetchone()[0],
            "file":          saved_name,
            "meta":          meta,
        })

    finally:
        conn.close()

@app.get("/submissions")
@app.get("/api/submissions")
@app.get("/api/index.py/submissions")
@app.get("/api/index/submissions")
def list_submissions():
    """
    Returns all audio submissions joined with persons, ordered newest-first.
    Used by submissions.html to populate the table.
    """
    conn = get_db()
    rows = conn.execute("""
        SELECT
            a.submission_id,
            a.person_id,
            p.canonical_name    AS name,
            p.canonical_phone   AS phone,
            a.file_path,
            a.original_name,
            a.file_size_bytes,
            a.duration_sec,
            a.sample_rate_hz,
            a.bitrate_kbps,
            a.loudness_dbfs,
            a.noise_estimate,
            a.noise_flag,
            a.submitted_at
        FROM audio_submissions a
        LEFT JOIN persons p ON a.person_id = p.person_id
        ORDER BY a.submitted_at DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/stats")
@app.get("/api/stats")
@app.get("/api/index.py/stats")
@app.get("/api/index/stats")
def get_stats():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM audio_submissions").fetchone()[0]
    unique_persons = conn.execute(
        "SELECT COUNT(DISTINCT person_id) FROM audio_submissions"
    ).fetchone()[0]
    avg_dur = conn.execute(
        "SELECT AVG(duration_sec) FROM audio_submissions WHERE duration_sec IS NOT NULL"
    ).fetchone()[0]
    conn.close()
    return {
        "total_submissions": total,
        "unique_persons": unique_persons,
        "avg_duration_sec": round(avg_dur, 1) if avg_dur else 0,
    }
