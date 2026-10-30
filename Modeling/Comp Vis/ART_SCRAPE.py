# ============================================================
# ALBUM ART LOOKUP - OVERNIGHT VERSION
# ============================================================


# ============================================================
# 1. IMPORTS
# ============================================================

import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from dotenv import load_dotenv
from rapidfuzz import fuzz
from sqlalchemy import create_engine, text
from tqdm import tqdm


# ============================================================
# 2. VARIABLE AND CREDENTIAL INITIALIZATION
# ============================================================

# Load environment variables from .env
load_dotenv()

# Database credentials and connection
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not configured in .env")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# API endpoints
MB_URL = "https://musicbrainz.org/ws/2/release/"
CAA_URL = "https://coverartarchive.org/release/"

# Matching configuration
MATCH_THRESHOLD = 70

# Processing configuration
MAX_WORKERS = 5
MAX_RETRIES = 4
REQUEST_TIMEOUT = 30

# API rate limits (minimum interval between request starts)
MB_REQUEST_INTERVAL = 1.2
CAA_REQUEST_INTERVAL = 1.0

# Output and logging paths
OUTPUT_DIR = "album_art_results"
RESULTS_FILE = os.path.join(OUTPUT_DIR, "album_art_lookup_results.csv")
LOG_FILE = "overnight_log.txt"
os.makedirs(OUTPUT_DIR, exist_ok=True)

USER_AGENT_EMAIL = os.getenv("MUSICBRAINZ_USER_AGENT_EMAIL")

if not USER_AGENT_EMAIL:
    raise RuntimeError("MUSICBRAINZ_USER_AGENT_EMAIL is not configured in .env")

USER_AGENT = f"AlbumArtResearch/1.0 ({USER_AGENT_EMAIL})"

# Output CSV structure
OUTPUT_COLUMNS = [
    "id",
    "album_name",
    "artist_name",
    "status",
    "release_id",
    "matched_title",
    "matched_artist",
    "artwork_url",
    "title_score",
    "artist_score",
    "score",
    "error",
]

# Statuses that may be worth retrying in a later run.
# Existing rows with these statuses are currently retained and skipped on resume,
# matching the previous chunk-based behavior.
RETRY_STATUSES = {"error", "artwork_check_error"}

# Thread-local HTTP sessions and shared synchronization controls
thread_local = threading.local()
CSV_LOCK = threading.Lock()
request_locks = {
    "mb": threading.Lock(),
    "caa": threading.Lock(),
}
last_request_times = {
    "mb": 0.0,
    "caa": 0.0,
}


# ============================================================
# 3. FUNCTIONS
# ============================================================

# ------------------------------------------------------------
# 3.1 Logging setup
# ------------------------------------------------------------

def setup_logging():
    """Configure logging to write to both a file and the console."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    return logging.getLogger(__name__)


# ------------------------------------------------------------
# 3.2 HTTP session and rate limiting
# ------------------------------------------------------------

def get_session():
    """Create one HTTP session per worker thread."""
    if not hasattr(thread_local, "session"):
        session = requests.Session()
        session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        })
        thread_local.session = session
    return thread_local.session


def wait_for_interval(kind):
    """Throttle request starts so we don't hammer external APIs."""
    min_interval = MB_REQUEST_INTERVAL if kind == "mb" else CAA_REQUEST_INTERVAL
    with request_locks[kind]:
        elapsed = time.monotonic() - last_request_times[kind]
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        last_request_times[kind] = time.monotonic()


# ------------------------------------------------------------
# 3.3 MusicBrainz request helper
# ------------------------------------------------------------

def musicbrainz_get(url, params=None, max_retries=MAX_RETRIES):
    """Send a MusicBrainz request with rate limiting and retries."""
    retryable_codes = (429, 500, 502, 503, 504)

    for attempt in range(max_retries):
        wait_for_interval("mb")
        try:
            response = get_session().get(url, params=params, timeout=REQUEST_TIMEOUT)

            if response.status_code in retryable_codes:
                if attempt == max_retries - 1:
                    response.raise_for_status()

                retry_after = response.headers.get("Retry-After")
                try:
                    wait_seconds = float(retry_after)
                    if wait_seconds <= 0:
                        raise ValueError("Retry-After must be positive")
                except (TypeError, ValueError):
                    wait_seconds = 10 * (2 ** attempt)

                logger.warning(
                    "MusicBrainz HTTP %s. Retrying after %.0f seconds.",
                    response.status_code,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                continue

            response.raise_for_status()
            return response

        except requests.RequestException as exc:
            if attempt == max_retries - 1:
                raise

            wait_seconds = 10 * (2 ** attempt)
            logger.warning(
                "MusicBrainz request failed: %s. Retrying in %.0f seconds.",
                exc,
                wait_seconds,
            )
            time.sleep(wait_seconds)

    raise RuntimeError("MusicBrainz request failed after all retries.")


# ------------------------------------------------------------
# 3.4 Artist credit extraction
# ------------------------------------------------------------

def get_credited_artist(release):
    """Extract the credited artist string from a MusicBrainz release result."""
    artist_parts = []

    for item in release.get("artist-credit", []):
        if isinstance(item, dict):
            artist_name = item.get("name", item.get("artist", {}).get("name", ""))
            artist_parts.append(artist_name)
            artist_parts.append(item.get("joinphrase", ""))
        elif isinstance(item, str):
            artist_parts.append(item)

    return "".join(artist_parts).strip()


# ------------------------------------------------------------
# 3.5 Album artwork lookup and matching
# ------------------------------------------------------------

def find_album_art(album_name, artist_name):
    """Search MusicBrainz, filter to releases with front-cover art, and rank the best match."""
    query = f'release:"{album_name}" AND artist:"{artist_name}"'

    response = musicbrainz_get(
        MB_URL,
        params={
            "query": query,
            "fmt": "json",
            "limit": 25,
        },
    )

    releases = response.json().get("releases", [])
    if not releases:
        return {"status": "not_found"}

    releases_with_art = []
    for release in releases:
        release_id = release.get("id")
        if not release_id:
            continue

        artwork_url = f"{CAA_URL}{release_id}/front-500"
        try:
            wait_for_interval("caa")
            cover_response = get_session().get(
                artwork_url,
                timeout=REQUEST_TIMEOUT,
                stream=True,
                allow_redirects=True,
            )
            try:
                if cover_response.status_code == 404:
                    continue
                cover_response.raise_for_status()
            finally:
                cover_response.close()
        except requests.RequestException as exc:
            return {
                "status": "artwork_check_error",
                "release_id": release_id,
                "error": str(exc),
            }

        releases_with_art.append({
            "release": release,
            "artwork_url": artwork_url,
        })

    if not releases_with_art:
        return {"status": "no_artwork"}

    candidates = []
    for item in releases_with_art:
        release = item["release"]
        candidate_title = release.get("title", "")
        candidate_artist = get_credited_artist(release)

        title_score = fuzz.ratio(
            str(album_name).casefold().strip(),
            candidate_title.casefold().strip(),
        )
        artist_score = fuzz.ratio(
            str(artist_name).casefold().strip(),
            candidate_artist.casefold().strip(),
        )
        combined_score = (0.6 * title_score) + (0.4 * artist_score)

        candidates.append({
            "release": release,
            "artwork_url": item["artwork_url"],
            "title_score": title_score,
            "artist_score": artist_score,
            "score": combined_score,
        })

    candidates.sort(key=lambda candidate: candidate["score"], reverse=True)
    best = candidates[0]

    if best["score"] < MATCH_THRESHOLD:
        return {
            "status": "below_threshold",
            "release_id": best["release"].get("id"),
            "matched_title": best["release"].get("title"),
            "matched_artist": get_credited_artist(best["release"]),
            "title_score": round(best["title_score"], 2),
            "artist_score": round(best["artist_score"], 2),
            "score": round(best["score"], 2),
        }

    return {
        "status": "matched",
        "release_id": best["release"].get("id"),
        "matched_title": best["release"].get("title"),
        "matched_artist": get_credited_artist(best["release"]),
        "artwork_url": best["artwork_url"],
        "title_score": round(best["title_score"], 2),
        "artist_score": round(best["artist_score"], 2),
        "score": round(best["score"], 2),
    }


# ------------------------------------------------------------
# 3.6 Database loading
# ------------------------------------------------------------

def fetch_albums_to_process():
    """Get the albums to be checked for artwork."""
    with engine.connect() as conn:
        try:
            query = text(
                """
                SELECT id, album_name, artist_name
                FROM merged23_25
                WHERE artwork_status IS NULL OR artwork_status = 'pending'
                ORDER BY id
                """
            )
            df = pd.read_sql(query, conn)
        except Exception:
            query = text(
                """
                SELECT id, album_name, artist_name
                FROM merged23_25
                ORDER BY id
                """
            )
            df = pd.read_sql(query, conn)

    if df.empty:
        logger.info("No albums available for processing.")

    return df[["id", "album_name", "artist_name"]].copy()


# ------------------------------------------------------------
# 3.7 Single-album processing
# ------------------------------------------------------------

def process_one_album(row):
    """Process a single album row and return a normalized result record."""
    result = {
        "id": row.id,
        "album_name": row.album_name,
        "artist_name": row.artist_name,
    }

    try:
        result.update(find_album_art(row.album_name, row.artist_name))
    except Exception as exc:
        result.update({
            "status": "error",
            "error": str(exc),
        })

    return result


# ------------------------------------------------------------
# 3.8 Thread-safe incremental CSV writing and resume support
# ------------------------------------------------------------

def initialize_results_file():
    """Create the results CSV with a header if it does not already exist."""
    if not os.path.exists(RESULTS_FILE) or os.path.getsize(RESULTS_FILE) == 0:
        pd.DataFrame(columns=OUTPUT_COLUMNS).to_csv(RESULTS_FILE, index=False)


def load_completed_ids():
    """Read IDs already present in the results file so completed work is skipped."""
    if not os.path.exists(RESULTS_FILE) or os.path.getsize(RESULTS_FILE) == 0:
        return set()

    existing = pd.read_csv(RESULTS_FILE, dtype={"id": str})
    if "id" not in existing.columns:
        raise ValueError(f"Results file is missing the 'id' column: {RESULTS_FILE}")

    return set(existing["id"].dropna().astype(str))


def append_result_to_csv(result):
    """Append one result safely, even if called by multiple worker threads."""
    result_df = pd.DataFrame([result], columns=OUTPUT_COLUMNS)

    with CSV_LOCK:
        result_df.to_csv(
            RESULTS_FILE,
            mode="a",
            header=False,
            index=False,
        )


# ------------------------------------------------------------
# 3.9 Continuous concurrent processing
# ------------------------------------------------------------

def process_one_album_and_save(row):
    """Process one album and immediately append its result to the shared CSV."""
    result = process_one_album(row)
    append_result_to_csv(result)
    return result


def process_all_albums(albums_to_process):
    """Process the full worklist concurrently, saving each result as it completes."""
    initialize_results_file()
    completed_ids = load_completed_ids()

    remaining = albums_to_process[
        ~albums_to_process["id"].astype(str).isin(completed_ids)
    ].copy()

    logger.info(
        "Loaded %s albums; %s already present in results; %s remaining.",
        len(albums_to_process),
        len(completed_ids),
        len(remaining),
    )

    if remaining.empty:
        logger.info("No remaining albums to process.")
        return pd.read_csv(RESULTS_FILE, dtype={"id": str})

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(process_one_album_and_save, row)
            for row in remaining.itertuples(index=False)
        ]

        with tqdm(total=len(futures), desc="Processing albums") as pbar:
            for future in as_completed(futures):
                # The worker saves its result before completing its future.
                # Calling result() here surfaces any unexpected worker exception.
                future.result()
                pbar.update(1)

    results_df = pd.read_csv(RESULTS_FILE, dtype={"id": str})
    logger.info("Finished processing. Results currently contain %s rows.", len(results_df))
    return results_df


# ============================================================
# 4. MAIN EXECUTION SETUP
# ============================================================

def main():
    """Initialize the run and process the complete album worklist."""
    global logger
    logger = setup_logging()

    logger.info("Starting album art lookup run.")

    # Load the database worklist only when the script is executed.
    albums_to_process = fetch_albums_to_process()

    if albums_to_process.empty:
        logger.info("No albums were selected for processing.")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    results_df = process_all_albums(albums_to_process)
    logger.info("Finished album art lookup run.")
    return results_df


# ============================================================
# 5. EXECUTION
# ============================================================

if __name__ == "__main__":
    main()
