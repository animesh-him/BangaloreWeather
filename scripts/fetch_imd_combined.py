#!/usr/bin/env python3
"""
fetch_imd_combined.py -- improved IMD extraction + open-meteo fetch.
Writes imd.json only when content changes.
"""

import os, json, time, re
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup

OUT_FILE = "imd.json"
LAT = float(os.environ.get("AERO_LAT", "12.9896"))
LON = float(os.environ.get("AERO_LON", "77.6387"))
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
IMD_BENGALURU_PAGE = os.environ.get("IMD_PAGE", "https://mausam.imd.gov.in/bengaluru/")
HEADERS = {"User-Agent":"github-actions-imd-fetcher/1.0 (+https://github.com/)", "Accept":"application/json,text/html"}
MAX_RETRIES = 3
TIMEOUT = 20

KEYWORDS = ["warning","watch","nowcast","thunderstorm","thunder","heavy rain","heavy rainfall","alert","advisory","likely","possible","isolated","severe","squall","gust"]

def now_iso():
    return datetime.now(timezone.utc).astimezone().isoformat()

def fetch_with_retries(url, params=None):
    backoff = 2
    for attempt in range(1, MAX_RETRIES+1):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
            if 200 <= r.status_code < 300:
                return r
            # do not retry unauthorized or too-many-requests aggressively
            if r.status_code in (401, 429):
                raise requests.HTTPError(f"HTTP {r.status_code} for {url}")
            if 500 <= r.status_code < 600:
                pass
            else:
                r.raise_for_status()
        except Exception as e:
            print(f"Attempt {attempt} error: {e}")
        if attempt < MAX_RETRIES:
            time.sleep(backoff); backoff *= 2
    raise SystemExit(f"Failed to fetch {url}")

def fetch_open_meteo(lat, lon):
    params = {
        "latitude": lat, "longitude": lon,
        "hourly": "temperature_2m,precipitation,precipitation_probability,windspeed_10m",
        "daily": "sunrise,sunset",
        "timezone": "auto"
    }
    r = fetch_with_retries(OPEN_METEO_URL, params=params)
    return r.json()

def extract_visible_paragraphs(html):
    soup = BeautifulSoup(html, "html.parser")
    # Remove noisy elements
    for sel in soup(["script","style","nav","header","footer","form","noscript","iframe","aside","svg","canvas"]):
        sel.decompose()
    # Prefer main/article/section
    for selector in ("main", "article", "section", "#content", ".content"):
        el = soup.select_one(selector)
        if el:
            # gather paragraphs within
            paras = [p.get_text(" ", strip=True) for p in el.find_all(["p","div","li","h2","h3"]) if p.get_text(strip=True)]
            if paras:
                return paras
    # fallback: use all paragraphs in body
    paras = [p.get_text(" ", strip=True) for p in soup.find_all(["p","div","li","h2","h3"]) if p.get_text(strip=True)]
    return paras

def is_nav_like(paragraph):
    # Reject if too short
    if len(paragraph) < 40:
        return True
    words = paragraph.split()
    # If paragraph contains many short words (likely navigation or headings) and short total words, reject
    short_words = sum(1 for w in words if len(w) <= 3)
    if len(words) < 40 and (short_words / max(1, len(words))) > 0.55:
        return True
    # If it's mostly single-word uppercase tokens, reject
    tokens = re.findall(r"[A-Za-z]{1,}", paragraph)
    if tokens and all(len(t) <= 4 for t in tokens) and len(tokens) < 30:
        return True
    return False

def contains_keywords(paragraph):
    low = paragraph.lower()
    return any(k in low for k in KEYWORDS)

def pick_warnings(paragraphs):
    candidates = []
    # First pass: paragraphs with keywords and that are not nav-like
    for p in paragraphs:
        p = re.sub(r'\s+', ' ', p).strip()
        if is_nav_like(p):
            continue
        if contains_keywords(p) and len(p) >= 40:
            candidates.append(p)
    # If we found candidate paragraphs, return them (dedup)
    if candidates:
        seen = set(); out=[]
        for c in candidates:
            if c in seen: continue
            seen.add(c); out.append(c)
            if len(out) >= 8: break
        return out
    # Second pass: try sentence-level search for keywords
    text = "\n\n".join(paragraphs)
    sentences = re.split(r'(?<=[.?!])\s+', text)
    found=[]
    for s in sentences:
        s = s.strip()
        if len(s) < 40: continue
        if contains_keywords(s):
            cleaned = re.sub(r'\s+', ' ', s)
            if cleaned not in found:
                found.append(cleaned)
        if len(found) >= 8:
            break
    return found

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
    # compare canonical JSON strings
    if existing is not None and json.dumps(existing, sort_keys=True, ensure_ascii=False) == json.dumps(payload, sort_keys=True, ensure_ascii=False):
        print("No change in imd.json — skipping write.")
        return False
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"Wrote {OUT_FILE}")
    return True

def fetch_imd_bengaluru():
    r = fetch_with_retries(IMD_BENGALURU_PAGE)
    html = r.text
    paras = extract_visible_paragraphs(html)
    warnings = pick_warnings(paras)
    # keep a trimmed extracted_text as fallback (first meaningful paragraphs joined)
    extracted = "\n\n".join(paras[:20])[:20000]
    return {"source_url": IMD_BENGALURU_PAGE, "warnings": warnings, "extracted_text": extracted}

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
        print("IMD fetch failed:", e)
        imd = {"source_url": IMD_BENGALURU_PAGE, "warnings": [], "extracted_text": ""}

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
