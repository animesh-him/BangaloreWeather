#!/usr/bin/env python3
"""
fetch_imd_combined.py — improved extraction for IMD Bengaluru page
Writes imd.json containing open_meteo + imd_bengaluru with 'warnings' array of good sentences.
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

def now_iso():
    return datetime.now(timezone.utc).astimezone().isoformat()

def fetch_with_retries(url, params=None):
    backoff = 2
    for attempt in range(1, MAX_RETRIES+1):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
            if 200 <= r.status_code < 300:
                return r
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
        "daily": "sunrise,sunset", "timezone": "auto"
    }
    r = fetch_with_retries(OPEN_METEO_URL, params=params)
    return r.json()

def extract_main_text(html):
    soup = BeautifulSoup(html, "html.parser")
    # remove noisy tags
    for s in soup(["script","style","nav","header","footer","form","noscript","iframe","aside"]):
        s.decompose()
    # prefer main/article/section
    for sel in ("main", "article", "section", "#content", ".content"):
        el = soup.select_one(sel)
        if el:
            text = el.get_text("\n", strip=True)
            if len(text) > 80:
                return text
    # fallback: combine all <p> into paragraphs
    paras = [p.get_text(" ", strip=True) for p in soup.find_all("p") if p.get_text(strip=True)]
    if paras:
        # join with double newline to denote paragraphs
        return "\n\n".join(paras)
    # ultimate fallback: body text
    body = soup.body
    return body.get_text(" ", strip=True) if body else ""

def pick_warning_paragraphs(text):
    if not text:
        return []
    # split into paragraphs by two newlines or by long lines
    paras = [p.strip() for p in re.split(r'\n{2,}|\r\n{2,}', text) if p.strip()]
    # keywords to look for
    keys = ["warning","watch","nowcast","thunderstorm","heavy rain","heavy rainfall","alert","advisory","likely","possible","isolated","severe"]
    found = []
    for p in paras:
        low = p.lower()
        # ignore short nav-like lines (very short or all caps single words)
        if len(p) < 40:
            continue
        # ignore paragraphs that are lists of short words (navigation)
        words = p.split()
        short_words_ratio = sum(1 for w in words if len(w)<=4) / max(1, len(words))
        if short_words_ratio > 0.6 and len(words) < 40:
            continue
        # if any keyword present, accept paragraph
        if any(k in low for k in keys):
            # collapse whitespace
            cleaned = re.sub(r'\s+', ' ', p).strip()
            found.append(cleaned)
    # if nothing found, fallback: try sentences search
    if not found:
        # split into sentences and pick those with keywords and decent length
        sents = re.split(r'(?<=[.?!])\s+', text)
        for s in sents:
            if len(s) < 30: continue
            low = s.lower()
            if any(k in low for k in keys):
                cleaned = re.sub(r'\s+', ' ', s).strip()
                found.append(cleaned)
    # deduplicate preserve order and limit
    seen = set(); unique = []
    for t in found:
        if t in seen: continue
        seen.add(t); unique.append(t)
        if len(unique) >= 8: break
    return unique

def save_if_changed(payload):
    try:
        old = None
        if os.path.exists(OUT_FILE):
            with open(OUT_FILE, "r", encoding="utf-8") as f: old = json.load(f)
        if old is not None and json.dumps(old, sort_keys=True, ensure_ascii=False) == json.dumps(payload, sort_keys=True, ensure_ascii=False):
            print("No change in imd.json — skipping write.")
            return False
        with open(OUT_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print("Wrote", OUT_FILE)
        return True
    except Exception as e:
        print("Error saving", e); return False

def main():
    print("Fetching Open-Meteo and IMD page...")
    om = {}
    try: om = fetch_open_meteo(LAT, LON)
    except Exception as e: print("Open-Meteo fetch failed:", e)
    imd_page = {}
    try:
        r = fetch_with_retries(IMD_BENGALURU_PAGE)
        html = r.text
        main_text = extract_main_text(html)
        warnings = pick_warning_paragraphs(main_text)
        imd_page = {"source_url": IMD_BENGALURU_PAGE, "warnings": warnings, "extracted_text": main_text[:20000]}
    except Exception as e:
        print("IMD page fetch failed:", e)
        imd_page = {"source_url": IMD_BENGALURU_PAGE, "warnings": [], "extracted_text": ""}

    payload = {
        "fetched_at": now_iso(),
        "location": {"name": "Aerospace Park, Bangalore", "latitude": LAT, "longitude": LON},
        "open_meteo": om,
        "imd_bengaluru": imd_page
    }
    saved = save_if_changed(payload)
    if saved: print("imd.json updated.")
    else: print("No update required.")

if __name__ == "__main__":
    main()
