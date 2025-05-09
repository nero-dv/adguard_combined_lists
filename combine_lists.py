#!/usr/bin/env python3

import asyncio
import aiohttp
import json
import hashlib
import redis.asyncio as redis  # For asyncio Redis client
from pathlib import Path
from typing import List, Set, Dict, Any, Tuple, Optional
import logging  # Import the logging module

# --- Logging Configuration ---
logging_directory = Path("/home/lemon/scripts/adguard_combined_lists/logs")
logging.basicConfig(
    filename=logging_directory, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# List of URLs for the .txt files to download.
URLS = [
    "https://adguardteam.github.io/HostlistsRegistry/assets/filter_30.txt",
    "https://adguardteam.github.io/HostlistsRegistry/assets/filter_11.txt",
    "https://adguardteam.github.io/HostlistsRegistry/assets/filter_50.txt",
    "https://adguardteam.github.io/HostlistsRegistry/assets/filter_3.txt",
    "https://adguardteam.github.io/HostlistsRegistry/assets/filter_44.txt",
    "https://raw.githubusercontent.com/hagezi/dns-blocklists/main/adblock/pro.plus.txt",
    "https://easylist.to/easylist/easyprivacy.txt",
]

# --- Configuration ---
COMMENT_PREFIX = "!"
COMBINED_LIST_FILENAME = "/home/lemon/scripts/adguard_combined_lists/adguard_combined_list.txt"
REQUEST_TIMEOUT_SECONDS = 60
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

# --- Redis Key Prefixes/Names ---
REDIS_KEY_URL_META_PREFIX = "list_meta:"
REDIS_KEY_URL_FILTERED_CONTENT_PREFIX = "list_filtered_content:"
REDIS_KEY_COMBINED_FINGERPRINT = "combined_list_fingerprint"


# --- Helper Functions (Checksums, Fingerprint, Line Filtering - mostly unchanged) ---
def calculate_content_checksum(content_bytes: bytes) -> str:
    logging.debug("Calculating content checksum.")
    return hashlib.sha256(content_bytes).hexdigest()


def calculate_fingerprint_from_checksums(checksums: List[str]) -> str:
    logging.debug("Calculating combined fingerprint from checksums.")
    if not checksums:
        logging.debug("No checksums provided for fingerprint calculation.")
        return ""
    concatenated_checksums = "".join(sorted(checksums))
    return hashlib.sha256(concatenated_checksums.encode("utf-8")).hexdigest()


def filter_raw_lines(raw_lines: List[str]) -> List[str]:
    logging.debug("Filtering raw lines.")
    filtered = []
    for line in raw_lines:
        stripped_line = line.strip()
        if stripped_line and not stripped_line.startswith(COMMENT_PREFIX):
            filtered.append(stripped_line)
    logging.debug(f"Filtered down to {len(filtered)} lines.")
    return filtered


# --- Core Logic using Redis ---


async def get_url_metadata_from_redis(redis_client: redis.Redis, url: str) -> Optional[Dict[str, str]]:
    """Fetches URL metadata from a Redis hash."""
    key = f"{REDIS_KEY_URL_META_PREFIX}{url}"
    logging.debug(f"Attempting to fetch metadata from Redis for {url} with key: {key}")
    meta_bytes = await redis_client.hgetall(key)
    if not meta_bytes:
        logging.debug(f"No metadata found in Redis for {url}")
        return None
    logging.debug(f"Successfully fetched metadata from Redis for {url}")
    return {k.decode("utf-8"): v.decode("utf-8") for k, v in meta_bytes.items()}


async def get_filtered_content_from_redis(redis_client: redis.Redis, url: str) -> Optional[List[str]]:
    """Fetches and decodes filtered list content (JSON string) from Redis."""
    key = f"{REDIS_KEY_URL_FILTERED_CONTENT_PREFIX}{url}"
    logging.debug(f"Attempting to fetch filtered content from Redis for {url} with key: {key}")
    json_data = await redis_client.get(key)
    if not json_data:
        logging.debug(f"No filtered content found in Redis for {url}")
        return None
    try:
        logging.debug(f"Successfully fetched filtered content from Redis for {url}. Decoding JSON.")
        return json.loads(json_data.decode("utf-8"))
    except json.JSONDecodeError:
        logging.warning(f"Could not decode JSON from Redis for content: {url}")
        return None


async def process_single_url(
    session: aiohttp.ClientSession,
    redis_client: redis.Redis,
    url: str,
    cached_url_data: Optional[Dict[str, str]],
) -> Tuple[Optional[List[str]], Optional[str], bool, Dict[str, Any]]:
    """
    Processes a single URL: checks Redis cache, downloads if necessary, checksums,
    and returns data for further processing and Redis update.
    """
    logging.info(f"Processing URL: {url}")
    has_changed = False
    raw_content_bytes: Optional[bytes] = None
    # raw_content_checksum will be of the raw downloaded content
    # etag and last_modified are from HTTP headers

    # 1. Try HEAD request for ETag/Last-Modified
    if cached_url_data:
        headers = {}
        if cached_url_data.get("etag"):
            headers["If-None-Match"] = cached_url_data["etag"]
            logging.debug(f"Adding If-None-Match header: {cached_url_data['etag']}")
        if cached_url_data.get("last_modified"):
            headers["If-Modified-Since"] = cached_url_data["last_modified"]
            logging.debug(f"Adding If-Modified-Since header: {cached_url_data['last_modified']}")

        if headers:
            try:
                logging.info(f"HEAD request for: {url}")
                async with session.head(
                    url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS, allow_redirects=True
                ) as response:
                    logging.info(f"HEAD response status for {url}: {response.status}")
                    if response.status == 304:  # Not Modified
                        logging.info(f"Cache hit (304 Not Modified) for: {url}")
                        # Try to get already filtered content from Redis
                        filtered_lines = await get_filtered_content_from_redis(redis_client, url)
                        if filtered_lines is not None:
                            logging.info(f"Using cached filtered content from Redis for {url}")
                            return (
                                filtered_lines,
                                cached_url_data["raw_content_checksum"],  # Trust cached checksum
                                False,  # Has not changed
                                cached_url_data,  # No change to metadata
                            )
                        else:
                            logging.warning(
                                f"304 for {url} but filtered content missing in Redis. Will re-download/process."
                            )
                    # If not 304, fall through to GET
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logging.warning(f"HEAD request failed for {url}: {e}. Proceeding to GET.")
            except Exception as e:
                logging.error(f"Unexpected error during HEAD request for {url}: {e}. Proceeding to GET.")

    # 2. Full GET request if needed
    try:
        logging.info(f"GET request for: {url}")
        async with session.get(url, timeout=REQUEST_TIMEOUT_SECONDS, allow_redirects=True) as response:
            response.raise_for_status()
            raw_content_bytes = await response.read()
            current_etag = response.headers.get("ETag")
            current_last_modified = response.headers.get("Last-Modified")
            logging.info(f"Successfully downloaded content for {url}.")
            logging.debug(f"ETag: {current_etag}, Last-Modified: {current_last_modified}")
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logging.error(f"Download failed for {url}: {e}")
        return None, None, True, {}  # Error, assume changed
    except Exception as e:
        logging.critical(f"Unexpected error downloading {url}: {e}")
        return None, None, True, {}

    if raw_content_bytes is None:  # Should be caught by exceptions, but safeguard
        logging.error(f"Raw content bytes are None after GET request for {url}. Skipping processing.")
        return None, None, True, {}

    current_raw_content_checksum = calculate_content_checksum(raw_content_bytes)
    logging.debug(f"Current raw content checksum for {url}: {current_raw_content_checksum}")

    # 3. Compare raw content checksum with cached one (if any)
    cached_checksum = cached_url_data.get("raw_content_checksum") if cached_url_data else None
    if cached_url_data and cached_checksum == current_raw_content_checksum:
        logging.info(f"Content unchanged (checksum match) for: {url}")
        # Try to get existing filtered content, or filter the content now if it's missing
        filtered_lines = await get_filtered_content_from_redis(redis_client, url)
        if filtered_lines is None:
            logging.warning(f"Re-filtering for {url} as cached filtered content was missing.")
            try:
                raw_text_content = raw_content_bytes.decode("utf-8", errors="replace")
            except UnicodeDecodeError:
                logging.warning(f"UTF-8 decoding failed for {url} during re-filtering, trying latin-1.")
                try:
                    raw_text_content = raw_content_bytes.decode("latin-1", errors="replace")
                except UnicodeDecodeError:
                    logging.error(
                        f"Critical: Could not decode content for {url} during re-filtering. Skipping this list's data."
                    )
                    return (
                        None,
                        current_raw_content_checksum,
                        True,
                        {},
                    )  # Treat as changed due to processing failure

            filtered_lines = filter_raw_lines(raw_text_content.splitlines())
            logging.debug(f"Saving newly filtered content to Redis for {url}")
            await redis_client.set(
                f"{REDIS_KEY_URL_FILTERED_CONTENT_PREFIX}{url}", json.dumps(filtered_lines)
            )
            has_changed = True  # Consider it a change if it had to re-process and save filtered data
        else:
            logging.debug(f"Using cached filtered content from Redis for {url} (checksum match).")

        # Metadata to save (saved even if content checksum same, ETag/Last-Modified might update)
        new_url_meta_for_redis = {
            "etag": current_etag or cached_url_data.get("etag") or "",
            "last_modified": current_last_modified or cached_url_data.get("last_modified") or "",
            "raw_content_checksum": current_raw_content_checksum,
        }
        logging.debug(f"Metadata for Redis for {url}: {new_url_meta_for_redis}")
        return filtered_lines, current_raw_content_checksum, has_changed, new_url_meta_for_redis
    else:
        logging.info(f"Content changed or new for: {url}")
        has_changed = True
        try:
            raw_text_content = raw_content_bytes.decode("utf-8")
            logging.debug(f"Decoded content for {url} using UTF-8.")
        except UnicodeDecodeError:
            logging.warning(f"UTF-8 decoding failed for {url}, trying latin-1.")
            try:
                raw_text_content = raw_content_bytes.decode("latin-1")
                logging.debug(f"Decoded content for {url} using latin-1.")
            except UnicodeDecodeError:
                logging.error(f"Critical: Could not decode content for {url}. Skipping this list.")
                return None, current_raw_content_checksum, True, {}

        logging.debug(f"Filtering lines for {url}.")
        filtered_lines = filter_raw_lines(raw_text_content.splitlines())
        logging.debug(f"Saving new filtered content to Redis for {url}")
        # Save new filtered content to reds
        await redis_client.set(f"{REDIS_KEY_URL_FILTERED_CONTENT_PREFIX}{url}", json.dumps(filtered_lines))

        new_url_meta_for_redis = {
            "etag": current_etag or "",
            "last_modified": current_last_modified or "",
            "raw_content_checksum": current_raw_content_checksum,
        }
        logging.debug(f"Metadata for Redis for {url}: {new_url_meta_for_redis}")
        return filtered_lines, current_raw_content_checksum, True, new_url_meta_for_redis


async def run_update_process():
    """
    Main process to update lists, using Redis for caching.
    """
    logging.info("Starting update process.")
    redis_client = None
    try:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        await redis_client.ping()
        logging.info(f"Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    except Exception as e:
        logging.critical(f"Could not connect to Redis at {REDIS_HOST}:{REDIS_PORT}. Error: {e}")
        logging.critical("Please ensure Redis server is running and accessible. Exiting.")
        return

    old_combined_fingerprint_bytes = await redis_client.get(REDIS_KEY_COMBINED_FINGERPRINT)
    old_combined_fingerprint = (
        old_combined_fingerprint_bytes.decode("utf-8") if old_combined_fingerprint_bytes else ""
    )
    logging.info(f"Old combined fingerprint from Redis: {old_combined_fingerprint or 'None'}")

    any_list_content_changed = False
    all_filtered_lines_from_sources: List[List[str]] = []
    current_individual_raw_checksums: List[str] = []
    updated_url_metadata_to_save_in_redis: Dict[str, Dict[str, str]] = {}
    has_processing_errors = False

    async with aiohttp.ClientSession() as session:
        tasks = []
        logging.info(f"Creating processing tasks for {len(URLS)} URLs.")
        for url in URLS:
            cached_url_meta = await get_url_metadata_from_redis(redis_client, url)
            tasks.append(process_single_url(session, redis_client, url, cached_url_meta))
        results = await asyncio.gather(*tasks)
        logging.info("Finished processing all URL tasks.")

    for i, url in enumerate(URLS):
        filtered_lines, raw_checksum, changed, new_meta_for_redis = results[i]

        if filtered_lines is None or raw_checksum is None:
            logging.error(
                f"Failed to process URL: {url}. Will mark as changed to ensure rebuild if necessary."
            )
            has_processing_errors = True
            # Attempt to use old data for this URL from redis if available, but still flag that a rebuild might be needed due to the error.
            old_filtered_content = await get_filtered_content_from_redis(redis_client, url)
            old_meta = await get_url_metadata_from_redis(redis_client, url)
            if old_filtered_content and old_meta and old_meta.get("raw_content_checksum"):
                logging.warning(f"Using stale Redis data for failed URL: {url}")
                all_filtered_lines_from_sources.append(old_filtered_content)
                current_individual_raw_checksums.append(old_meta["raw_content_checksum"])
                updated_url_metadata_to_save_in_redis[url] = old_meta  # Keep old meta
            else:
                logging.warning(
                    f"No usable stale data in Redis for failed URL: {url}. This list will be missing or incomplete."
                )
            any_list_content_changed = True  # Force rebuild due to error
            continue

        all_filtered_lines_from_sources.append(filtered_lines)
        current_individual_raw_checksums.append(raw_checksum)
        updated_url_metadata_to_save_in_redis[url] = new_meta_for_redis
        if changed:
            any_list_content_changed = True
            logging.info(f"Changes detected for URL: {url}")

    # --- Decide if a full rebuild of combined_list.txt is needed ---
    rebuild_needed = False
    if any_list_content_changed:
        logging.info("Reason to rebuild: One or more list contents changed or processed anew.")
        rebuild_needed = True

    current_combined_fingerprint = calculate_fingerprint_from_checksums(current_individual_raw_checksums)
    logging.info(f"Current combined fingerprint of processed lists: {current_combined_fingerprint or 'None'}")

    if old_combined_fingerprint != current_combined_fingerprint:
        logging.info("Reason to rebuild: Combined fingerprint of all lists changed.")
        rebuild_needed = True

    if not Path(COMBINED_LIST_FILENAME).exists():
        logging.info(f"Reason to rebuild: Output file '{COMBINED_LIST_FILENAME}' is missing.")
        rebuild_needed = True

    if (
        has_processing_errors and not rebuild_needed
    ):  # If errors occurred but no other change triggered rebuild
        logging.warning("Reason to rebuild: Errors occurred during list processing.")
        rebuild_needed = True

    if rebuild_needed:
        logging.info("Rebuilding combined list...")
        final_unique_lines: Set[str] = set()
        for lines_list in all_filtered_lines_from_sources:
            if lines_list:  # Ensure the list is not None or empty from failed processing
                final_unique_lines.update(lines_list)

        try:
            with open(COMBINED_LIST_FILENAME, "w", encoding="utf-8") as f:
                for line in sorted(list(final_unique_lines)):
                    f.write(line + "\n")
            logging.info(
                f"Successfully wrote {len(final_unique_lines)} unique lines to {COMBINED_LIST_FILENAME}"
            )

            # Update Redis metadata
            logging.info("Updating Redis cache metadata.")
            for url, meta_dict in updated_url_metadata_to_save_in_redis.items():
                # Ensure all values in meta_dict are strings for hmset
                stringified_meta = {k: str(v) for k, v in meta_dict.items() if v is not None}
                if stringified_meta:  # only set if there's something to set
                    await redis_client.hset(f"{REDIS_KEY_URL_META_PREFIX}{url}", mapping=stringified_meta)
                    logging.debug(f"Updated metadata in Redis for {url}")
                else:
                    logging.warning(f"No metadata to save for {url}")

            await redis_client.set(REDIS_KEY_COMBINED_FINGERPRINT, current_combined_fingerprint)
            logging.info("Redis combined fingerprint updated.")

        except IOError as e:
            logging.critical(f"Error writing combined list to {COMBINED_LIST_FILENAME}: {e}")
            logging.warning("Redis cache metadata NOT updated due to write error.")
        except Exception as e:
            logging.critical(f"An unexpected error occurred during file writing or Redis update: {e}")
            logging.warning("Redis cache metadata update might be incomplete due to this error.")
    else:
        logging.info(f"No changes detected in source lists. '{COMBINED_LIST_FILENAME}' is up-to-date.")

    if redis_client:
        await redis_client.close()
        logging.info("Redis connection closed.")

    logging.info("Update process finished.")


if __name__ == "__main__":
    asyncio.run(run_update_process())
