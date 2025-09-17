#!/usr/bin/env python3
"""
fetch_imd_combined.py
- Fetches Open-Meteo hourly forecast for a lat/lon (defaults to Aerospace Park, Bangalore).
- Fetches IMD Bengaluru public page (HTML) and extracts advisory/warning sentences.
- Writes imd.json with merged content only if it changes.
"""

import os, json, time, re
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

# ---------- CONFIG ----------
OUT_FILE = "imd.json"
# Default location: Aerospace Park, Bangalore
LAT = float(os.environ.get("AERO_LAT", "12.9896"))
LON = float(os.environ.get("AERO_LON", "77.6387"))
# Open-Meteo URL (no key required)
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
# Public IMD Bengaluru page (safe public HTML)
IMD_BENGALURU_PAGE = os.environ.get("IMD_PAGE", "https://mausam.imd.gov.in/bengaluru/")
# Polite headers
HEADERS = {"User-Agent": "github-actions-imd-fetcher/1.0 (+https://github.com/)", "Accept":"application/json,text/html"}
# Retry settings
MAX_RETRIES = 3
TIMEOUT = 20
# ----------------------------

def now_iso():
    return datetime.now(timezone.utc).astimezone().isoformat()

def fetch_url(url, params=None):
    backoff = 2
    for attempt in range(1, MAX_RETRIES+1):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
            if 200 <= r.status_code < 300:
                return r
            # For 429 or 401, do not hammer
            if r.status_code in (401, 429):
                raise requests.HTTPError(f"HTTP {r.status_code} for {url}")
            # For server errors, retry
            if 500 <= r.status_code < 600:
                pass
            else:
                r.raise_for_status()
        except Exception as e:
            print(f"Attempt {attempt} failed: {e}")
        if attempt < MAX_RETRIES:
            time.sleep(backoff)
            backoff *= 2
    raise SystemExit(f"Failed to fetch {url} after retries")

def fetch_open_meteo(lat, lon):
    params = {
        "latitude": lat, "longitude": lon,
        "hourly": "temperature_2m,precipitation,precipitation_probability,windspeed_10m",
        "daily": "sunrise,sunset",
        "timezone": "auto"
    }
    r = fetch_url(OPEN_METEO_URL, params=params)
    return r.json()

def extract_imd_text(html):
    soup = BeautifulSoup(html, "html.parser")
    for s in soup(["script","style","nav","header","footer","form","noscript","iframe"]):
        s.decompose()
    # Try main/article first
    for sel in ("main", "article", "section", ".content", "#content"):
        el = soup.select_one(sel)
        if el:
            text = el.get_text(separator="\n", strip=True)
            if len(text) > 40:
                return text
    # Fallback: gather paragraphs and filter
    paragraphs = [p.get_text(strip=True) for p in soup.find_all("p")]
    text = "\n\n".join(p for p in paragraphs if p)
    return text[:20000]

def find_warning_sentences(text):
    if not text:
        return []
    keys = ["warning","watch","nowcast","thunderstorm","heavy rain","heavy rainfall","alert","advisory","likely","possible","isolated"]
    sents = re.split(r'(?<=[.\n])\s+', text)
    found = []
    for s in sents:
        low = s.lower()
        for k in keys:
            if k in low and len(s.strip())>10:
                snippet = re.sub(r'\s+', ' ', s.strip())
                if snippet not in found:
                    found.append(snippet)
    return found[:12]

def fetch_imd_bengaluru():
    r = fetch_url(IMD_BENGALURU_PAGE)
    html = r.text
    text = extract_imd_text(html)
    warnings = find_warning_sentences(text)
    return {"source_url": IMD_BENGALURU_PAGE, "extracted_text": text[:20000], "warnings": warnings}

def load_existing():
    if not os.path.exists(OUT_FILE):
        return None
    try:
        with open(OUT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def save_if_changed(payload):
    existing = load_existing()
    if existing is not None and json.dumps(existing, sort_keys=True, ensure_ascii=False) == json.dumps(payload, sort_keys=True, ensure_ascii=False):
        print("No change in imd.json — skipping write.")
        return False
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"Wrote {OUT_FILE}")
    return True

def main():
    print("Fetching Open-Meteo and IMD Bengaluru page...")
    om = {}
    try:
        om = fetch_open_meteo(LAT, LON)
    except Exception as e:
        print("Open-Meteo fetch failed:", e)
    imd = {}
    try:
        imd = fetch_imd_bengaluru()
    except Exception as e:
        print("IMD Bengaluru fetch failed:", e)

    out = {
        "fetched_at": now_iso(),
        "location": {"name": "Aerospace Park, Bangalore", "latitude": LAT, "longitude": LON},
        "open_meteo": om,
        "imd_bengaluru": imd
    }

    changed = save_if_changed(out)
    if not changed:
        print("No commit needed.")
    else:
        print("imd.json updated.")

if __name__ == "__main__":
    main()
