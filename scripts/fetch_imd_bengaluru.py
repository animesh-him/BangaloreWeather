#!/usr/bin/env python3
"""
fetch_imd_bengaluru.py

Fetch IMD district API (IMD_SOURCE_URL). If JSON is returned, extract the Bengaluru
(district) entry and write a concise imd.json that contains:
{
  "fetched_at": "...",
  "source_url": "...",
  "district_key_matched": "...",   # matching key name used (if found)
  "bengaluru": { ... }             # the IMD district entry for Bengaluru (or null)
}

If the endpoint returns HTML, script falls back to extracting readable text and
searches for Bengaluru terms heuristically.

The script is written to:
 - retry with exponential backoff on transient errors,
 - use a polite User-Agent,
 - write imd.json only if content differs from existing file (to avoid needless commits).
"""

import os
import sys
import time
import json
import requests
from datetime import datetime, timezone
from bs4 import BeautifulSoup

# Config from env
IMD_SOURCE_URL = os.environ.get("IMD_SOURCE_URL", "").strip()
IMD_DISTRICT_NAME = os.environ.get("IMD_DISTRICT_NAME", "Bengaluru").strip()
IMD_STATE_NAME = os.environ.get("IMD_STATE_NAME", "Karnataka").strip()
OUT_FILE = "imd.json"

# Polite request headers
HEADERS = {
    "User-Agent": "github-actions-imd-fetcher/1.0 (+https://github.com/) - polite automated fetcher for public data",
    "Accept": "application/json, text/html, */*"
}

# retry/backoff configuration
MAX_RETRIES = 3
INITIAL_BACKOFF = 3  # seconds
TIMEOUT = 20  # seconds

def pretty_now_iso():
    return datetime.now(timezone.utc).astimezone().isoformat()

def fetch_with_retries(url):
    backoff = INITIAL_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            # If server asks us to slow down (429) be polite: stop and raise
            if resp.status_code == 429:
                print(f"Received 429 Too Many Requests on attempt {attempt}; will abort to avoid blocking.", file=sys.stderr)
                resp.raise_for_status()
            # Accept 200..299
            if 200 <= resp.status_code < 300:
                return resp
            else:
                print(f"Unexpected status {resp.status_code} on attempt {attempt}", file=sys.stderr)
                # For 5xx, retry; for 4xx (other than 429) probably won't help, so break
                if 500 <= resp.status_code < 600:
                    # server error — retry
                    pass
                else:
                    # client error — do not retry
                    resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Request attempt {attempt} error: {e}", file=sys.stderr)
            # continue to retry if attempts left
        if attempt < MAX_RETRIES:
            sleep = backoff
            print(f"Sleeping {sleep}s before retry #{attempt+1}...", file=sys.stderr)
            time.sleep(sleep)
            backoff *= 2
    raise SystemExit("Failed to fetch after retries")

def try_parse_json(resp):
    try:
        return resp.json()
    except Exception:
        return None

def find_bengaluru_in_district_json(data):
    """
    IMD district JSON likely contains an array or dict keyed by district/state.
    We try several heuristics:
      - If top-level is list: look for items where 'district' or 'districtName' contains Bengaluru/Bangalore
      - If top-level is dict of district objects keyed by id: inspect values.
      - Match on IMD_DISTRICT_NAME or common variants.
    Returns (matched_key, matched_obj) or (None, None)
    """
    name_lower = IMD_DISTRICT_NAME.lower()
    alt_names = {name_lower, "bengaluru urban", "bengaluru", "bengaluru city", "bangalore", "bangalore urban"}
    # If list
    if isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                continue
            # try multiple possible field names
            for key in ("district", "district_name", "districtName", "district_name_text", "district_name_en"):
                val = item.get(key) if isinstance(item, dict) else None
                if isinstance(val, str) and val.strip().lower() in alt_names:
                    return val.strip(), item
            # fallback: search any string fields
            for v in item.values():
                if isinstance(v, str) and v.strip().lower() in alt_names:
                    return v.strip(), item
        # not found
    elif isinstance(data, dict):
        # common pattern: dict of objects keyed by id
        for k, v in data.items():
            if isinstance(v, dict):
                # check fields inside v
                for key in ("district", "district_name", "districtName"):
                    val = v.get(key)
                    if isinstance(val, str) and val.strip().lower() in alt_names:
                        return k, v
                # fallback search values
                for vi in v.values():
                    if isinstance(vi, str) and vi.strip().lower() in alt_names:
                        return k, v
        # As alternative: maybe the data itself is keyed by state -> districts
        # Try recursively: if values are dicts that contain dicts/lists with district entries
        for k, v in data.items():
            # if nested list/dict, try to scan
            if isinstance(v, (list, dict)):
                mk, mv = find_bengaluru_in_district_json(v)
                if mk:
                    return mk, mv
    return None, None

def extract_text_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    # remove scripts/styles
    for s in soup(["script", "style", "header", "footer", "nav", "noscript", "form", "iframe"]):
        s.decompose()
    # prefer main/article
    for selector in ("main", "article", "section", "#content", ".content", ".article"):
        el = soup.select_one(selector)
        if el:
            txt = el.get_text(separator="\n", strip=True)
            if len(txt) > 50:
                return txt
    # fallback body
    body = soup.body
    if body:
        txt = body.get_text(separator="\n", strip=True)
        return txt[:20000]
    return ""

def load_existing_imd():
    if not os.path.exists(OUT_FILE):
        return None
    try:
        with open(OUT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def save_if_changed(payload):
    # Compare against existing file; write only if different
    existing = load_existing_imd()
    if existing is not None:
        # simple string compare of pretty JSON
        if json.dumps(existing, sort_keys=True, ensure_ascii=False) == json.dumps(payload, sort_keys=True, ensure_ascii=False):
            print("No change in imd.json content — skipping write/commit.")
            return False
    # write file
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"Wrote {OUT_FILE}")
    return True

def main():
    if not IMD_SOURCE_URL:
        print("IMD_SOURCE_URL is not set. Set repository variable IMD_SOURCE_URL to the IMD endpoint.", file=sys.stderr)
        sys.exit(1)

    print(f"Fetching IMD source: {IMD_SOURCE_URL}")
    resp = fetch_with_retries(IMD_SOURCE_URL)
    # Try parse JSON
    j = try_parse_json(resp)
    output = {
        "fetched_at": pretty_now_iso(),
        "source_url": IMD_SOURCE_URL,
        "source_type": None,
        "district_key_matched": None,
        "bengaluru": None
    }

    if j is not None:
        output["source_type"] = "json"
        # Try to find Bengaluru entry
        matched_key, matched_obj = find_bengaluru_in_district_json(j)
        if matched_obj:
            output["district_key_matched"] = matched_key
            output["bengaluru"] = matched_obj
            output["note"] = f"Matched district '{matched_key}' by heuristic"
            print("Matched Bengaluru entry in JSON.")
        else:
            # If not found, try to look for state/district naming inside nested structure
            # fallback: store entire payload under 'data' but attempt to find best guess
            output["bengaluru"] = None
            output["data"] = j
            print("Could not find explicit Bengaluru district entry in JSON. Stored full payload under 'data'.")
    else:
        # HTML fallback: extract readable text and look for Bengaluru mentions
        output["source_type"] = "html"
        text = extract_text_from_html(resp.text)
        output["extracted_text"] = text
        # search for lines mentioning Bengaluru
        lines = [ln.strip() for ln in text.splitlines() if 'bengalur' in ln.lower() or 'bangalore' in ln.lower()]
        output["bengaluru_mentions"] = lines[:20]
        if lines:
            print("Found Bengaluru mentions in HTML.")
        else:
            print("No Bengaluru-specific text found in HTML fallback.")

    # Save only if changed
    saved = save_if_changed(output)
    if saved:
        print("imd.json updated (committed by workflow).")
    else:
        print("imd.json unchanged; no commit necessary.")

if __name__ == "__main__":
    main()
