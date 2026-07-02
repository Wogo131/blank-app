#!/usr/bin/env python3
"""Chrome Web Store opportunity mining via the Chrome-Stats API.

Pipeline:
  1. "Ripe for disruption" query : big-ish user base, mediocre rating, stale update.
  2. "Rising stars" query        : young extensions growing fast with great ratings.
  3. Opportunity scoring of ripe candidates.
  4. Review complaint analysis for the top 20 ripe candidates.
  5. mining_output/report.md + two CSVs.

Usage:
    CHROME_STATS_API_KEY=<key> python3 mining/mine.py

Constraints handled:
  - hard budget of MAX_REQUESTS API calls (aborts gracefully past it)
  - 429 handling with Retry-After / exponential backoff
  - schema-error recovery: if a condition column is rejected, known variants
    are tried; if none works the condition is dropped and enforced client-side.
"""

import csv
import json
import math
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

API_BASE = "https://chrome-stats.com"
API_KEY = os.environ.get("CHROME_STATS_API_KEY", "")
OUT_DIR = Path(__file__).resolve().parent.parent / "mining_output"
STATE_DIR = OUT_DIR / "raw"

MAX_REQUESTS = 290          # stay under 300
PAGES_PER_QUERY = 8         # 5-10 pages requested
TOP_REVIEWS = 20            # ripe candidates whose reviews we fetch
NOW = datetime.now(timezone.utc)
STALE_CUTOFF = NOW - timedelta(days=int(18 * 30.44))    # 18 months ago
YOUNG_CUTOFF = NOW - timedelta(days=365)                # 12 months ago

request_count = 0
session = requests.Session()
session.headers.update({"x-api-key": API_KEY, "Content-Type": "application/json"})


class BudgetExceeded(Exception):
    pass


def api_call(method, path, *, json_body=None, params=None, max_retries=5):
    """Single API call with budget accounting and 429 backoff."""
    global request_count
    if request_count >= MAX_REQUESTS:
        raise BudgetExceeded(f"request budget of {MAX_REQUESTS} reached")
    backoff = 2
    for attempt in range(max_retries):
        request_count += 1
        resp = session.request(method, API_BASE + path, json=json_body,
                               params=params, timeout=60)
        if resp.status_code == 429:
            wait = float(resp.headers.get("Retry-After", backoff))
            print(f"  429 rate-limited, sleeping {wait:.0f}s "
                  f"(attempt {attempt + 1}/{max_retries})")
            time.sleep(wait)
            backoff = min(backoff * 2, 60)
            continue
        return resp
    raise RuntimeError(f"still rate-limited after {max_retries} attempts: {path}")


# ---------------------------------------------------------------------------
# Step 1 & 2 — advanced search with schema-error recovery
# ---------------------------------------------------------------------------

# Known naming variants per logical column, tried in order.
COLUMN_VARIANTS = {
    "userCount":    ["userCount", "users", "installCount"],
    "ratingValue":  ["ratingValue", "rating", "averageRating"],
    "ratingCount":  ["ratingCount", "ratingsCount", "reviewCount", "numRatings"],
    "lastUpdate":   ["lastUpdate", "lastUpdated", "updatedAt", "updateDate",
                     "lastUpdateDate"],
    "creationDate": ["creationDate", "createdAt", "publishedAt", "firstSeen",
                     "firstReleaseDate"],
}
# Value encodings tried for date columns (callable of datetime -> value).
DATE_ENCODINGS = [
    lambda d: d.strftime("%Y-%m-%d"),
    lambda d: d.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
    lambda d: int(d.timestamp() * 1000),
    lambda d: int(d.timestamp()),
]


def looks_like_schema_error(resp):
    if resp.status_code in (400, 422):
        return True
    if resp.status_code == 200:
        try:
            body = resp.json()
        except ValueError:
            return False
        if isinstance(body, dict) and body.get("error"):
            return True
    return False


def run_search(logical_conditions, sorting="userCount", direction="desc",
               pages=PAGES_PER_QUERY, label=""):
    """Run a paginated advanced-search.

    logical_conditions: list of dicts
        {"logical": <name in COLUMN_VARIANTS>, "operator": op, "value": v,
         "is_date": bool}
    Returns (rows, dropped_conditions).
    """
    resolved = {}      # logical name -> (column, encoder or None)
    dropped = []

    def build_conditions():
        out = []
        for c in logical_conditions:
            if c["logical"] in dropped:
                continue
            col, enc = resolved.get(c["logical"],
                                    (COLUMN_VARIANTS[c["logical"]][0], None))
            val = c["value"]
            if c.get("is_date"):
                encoder = enc or DATE_ENCODINGS[0]
                val = encoder(val)
            out.append({"column": col, "operator": c["operator"], "value": val})
        return out

    def attempt(page):
        body = {"sorting": sorting, "sortDirection": direction,
                "index": "extension", "page": page,
                "fields": {"operator": "AND", "conditions": build_conditions()}}
        return api_call("POST", "/api/chrome/advanced-search", json_body=body)

    # --- schema probing on page 1 -----------------------------------------
    resp = attempt(1)
    if looks_like_schema_error(resp):
        print(f"[{label}] schema error on first try: "
              f"HTTP {resp.status_code} {resp.text[:300]}")
        # Probe each condition independently to find the culprit(s).
        for c in logical_conditions:
            ok = False
            for col in COLUMN_VARIANTS[c["logical"]]:
                encoders = DATE_ENCODINGS if c.get("is_date") else [None]
                for enc in encoders:
                    val = enc(c["value"]) if enc else c["value"]
                    body = {"sorting": "userCount", "sortDirection": "desc",
                            "index": "extension", "page": 1,
                            "fields": {"operator": "AND", "conditions": [
                                {"column": col, "operator": c["operator"],
                                 "value": val}]}}
                    r = api_call("POST", "/api/chrome/advanced-search",
                                 json_body=body)
                    if not looks_like_schema_error(r):
                        resolved[c["logical"]] = (col, enc)
                        ok = True
                        break
                if ok:
                    break
            if not ok:
                print(f"[{label}] dropping condition {c['logical']} "
                      f"(no accepted variant) — will filter client-side")
                dropped.append(c["logical"])
        resp = attempt(1)
        if looks_like_schema_error(resp):
            raise RuntimeError(f"[{label}] search still failing after probing: "
                               f"{resp.status_code} {resp.text[:300]}")

    rows = []

    def extract(resp):
        data = resp.json()
        if isinstance(data, list):
            return data
        for key in ("data", "results", "items", "extensions", "hits"):
            if isinstance(data.get(key), list):
                return data[key]
        return []

    page_rows = extract(resp)
    rows.extend(page_rows)
    print(f"[{label}] page 1: {len(page_rows)} rows")

    for page in range(2, pages + 1):
        if not page_rows:
            break
        resp = attempt(page)
        if resp.status_code != 200:
            print(f"[{label}] page {page}: HTTP {resp.status_code}, stopping")
            break
        page_rows = extract(resp)
        rows.extend(page_rows)
        print(f"[{label}] page {page}: {len(page_rows)} rows")
        if not page_rows:
            break
    return rows, dropped


# ---------------------------------------------------------------------------
# Row normalisation and client-side filtering
# ---------------------------------------------------------------------------

def g(row, *names, default=None):
    for n in names:
        if n in row and row[n] is not None:
            return row[n]
    return default


def parse_date(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        ts = v / 1000 if v > 1e11 else v
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    s = str(v)
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d",
                "%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def normalize(row):
    ext_id = g(row, "id", "extensionId", "extId")
    return {
        "id": ext_id,
        "name": g(row, "name", "title", default=""),
        "users": g(row, "userCount", "users", "installCount", default=0) or 0,
        "rating": float(g(row, "ratingValue", "rating", "averageRating",
                          default=0) or 0),
        "rating_count": g(row, "ratingCount", "ratingsCount", "reviewCount",
                          "numRatings", default=0) or 0,
        "last_update": parse_date(g(row, "lastUpdate", "lastUpdated",
                                    "updatedAt", "updateDate")),
        "created": parse_date(g(row, "creationDate", "createdAt", "publishedAt",
                                "firstSeen")),
        "author": str(g(row, "author", "developer", "offeredBy", "publisher",
                        default="") or ""),
        "category": g(row, "category", "categoryName", default=""),
        "url": f"https://chromewebstore.google.com/detail/{ext_id}",
        "stats_url": f"https://chrome-stats.com/d/{ext_id}",
        "raw": row,
    }


def is_google(n):
    a = n["author"].lower()
    return "google" in a or "google" in str(n["raw"].get("email", "")).lower()


def ripe_filter(n):
    return (n["users"] >= 10_000 and n["users"] <= 3_000_000
            and 2.3 <= n["rating"] <= 4.2 and n["rating_count"] >= 30
            and (n["last_update"] is None or n["last_update"] <= STALE_CUTOFF)
            and not is_google(n))


def rising_filter(n):
    return (1_000 <= n["users"] <= 80_000 and n["rating"] >= 4.4
            and n["rating_count"] >= 10
            and (n["created"] is not None and n["created"] >= YOUNG_CUTOFF)
            and not is_google(n))


# ---------------------------------------------------------------------------
# Step 3 — scoring
# ---------------------------------------------------------------------------

def score(n):
    months_stale = 18.0
    if n["last_update"]:
        months_stale = (NOW - n["last_update"]).days / 30.44
    # abandonment factor: 1.0 at exactly 18 months, +1 per extra year, cap 3.
    abandon = min(1.0 + max(0.0, months_stale - 18.0) / 12.0, 3.0)
    return (math.log10(max(n["users"], 10))
            * max(4.6 - n["rating"], 0.05)
            * math.log10(max(n["rating_count"], 10))
            * abandon)


# ---------------------------------------------------------------------------
# Step 4 — review complaint mining
# ---------------------------------------------------------------------------

COMPLAINT_PATTERNS = {
    "broken":       r"\b(broken|doesn'?t work|does not work|stopped working|"
                    r"not working|no longer works?|ne (marche|fonctionne) plus|"
                    r"quit working|useless now)\b",
    "perf":         r"\b(crash(es|ed|ing)?|slow|lag(gy|s|ging)?|freez(es|ing)?|"
                    r"memory|\bram\b|cpu|lent|plante)\b",
    "ads":          r"\b(ads?\b|advert|popup|pop-up|spam|sponsored|pub(licit\w*)?)\b",
    "paywall":      r"\b(paywall|pay wall|subscription|premium|paid now|"
                    r"pay to|payant|abonnement|charge|pricing)\b",
    "forced_login": r"\b(login|log in|sign ?in|sign ?up|account required|"
                    r"force[ds]? (me )?to (log|sign)|connexion obligatoire)\b",
    "no_sync":      r"\b(sync|synchroni[sz]|cloud|backup|across devices)\b",
    "feature_req":  r"\b(wish|would be (nice|great)|please add|missing|"
                    r"feature request|needs? (an? )?option|it lacks|manque)\b",
    "abandoned":    r"\b(abandoned|no updates?|not (been )?updated|dead|"
                    r"developer (is )?gone|no support|no response|unmaintained|"
                    r"plus de mise[s]? à jour)\b",
}


def extract_reviews(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("reviews", "data", "results", "items"):
            if isinstance(payload.get(key), list):
                return payload[key]
    return []


def analyze_reviews(ext_id):
    resp = api_call("GET", "/api/reviews", params={"id": ext_id})
    if resp.status_code != 200:
        return {"error": f"HTTP {resp.status_code}"}
    reviews = extract_reviews(resp.json())
    counts = {k: 0 for k in COMPLAINT_PATTERNS}
    quotes = []
    total = 0
    for rv in reviews:
        text = str(g(rv, "text", "comment", "content", "review", default="") or "")
        rating = g(rv, "rating", "stars", "ratingValue", default=None)
        if not text:
            continue
        total += 1
        low = text.lower()
        for key, pat in COMPLAINT_PATTERNS.items():
            if re.search(pat, low):
                counts[key] += 1
        try:
            rnum = float(rating) if rating is not None else None
        except (TypeError, ValueError):
            rnum = None
        if rnum is not None and rnum <= 2 and len(quotes) < 3 and len(text) > 25:
            snippet = re.sub(r"\s+", " ", text).strip()
            if len(snippet) > 220:
                snippet = snippet[:217] + "..."
            quotes.append({"rating": int(rnum), "text": snippet})
    return {"total_analyzed": total, "counts": counts, "quotes": quotes}


# ---------------------------------------------------------------------------
# Step 5 — outputs
# ---------------------------------------------------------------------------

CSV_FIELDS = ["rank", "id", "name", "users", "rating", "rating_count",
              "last_update", "created", "months_since_update", "score",
              "author", "category", "url", "stats_url"]


def to_csv_row(rank, n, sc=None):
    months = ""
    if n["last_update"]:
        months = round((NOW - n["last_update"]).days / 30.44, 1)
    return {
        "rank": rank, "id": n["id"], "name": n["name"], "users": n["users"],
        "rating": n["rating"], "rating_count": n["rating_count"],
        "last_update": n["last_update"].date().isoformat() if n["last_update"] else "",
        "created": n["created"].date().isoformat() if n["created"] else "",
        "months_since_update": months,
        "score": round(sc, 2) if sc is not None else "",
        "author": n["author"], "category": n["category"],
        "url": n["url"], "stats_url": n["stats_url"],
    }


def write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        w.writerows(rows)


COMPLAINT_LABELS = {
    "broken": "cassé / ne marche plus", "perf": "crash / lenteur / RAM",
    "ads": "publicités", "paywall": "paywall / passage payant",
    "forced_login": "login forcé", "no_sync": "sync manquante",
    "feature_req": "demandes de fonctionnalités", "abandoned": "dev disparu",
}


def main():
    if not API_KEY:
        sys.exit("CHROME_STATS_API_KEY is not set")
    OUT_DIR.mkdir(exist_ok=True)
    STATE_DIR.mkdir(exist_ok=True)

    # ---- Step 1: ripe for disruption -------------------------------------
    ripe_conditions = [
        {"logical": "userCount", "operator": ">=", "value": 10_000},
        {"logical": "userCount", "operator": "<=", "value": 3_000_000},
        {"logical": "ratingValue", "operator": ">=", "value": 2.3},
        {"logical": "ratingValue", "operator": "<=", "value": 4.2},
        {"logical": "ratingCount", "operator": ">=", "value": 30},
        {"logical": "lastUpdate", "operator": "<=", "value": STALE_CUTOFF,
         "is_date": True},
    ]
    ripe_raw, _ = run_search(ripe_conditions, label="ripe")
    (STATE_DIR / "ripe_raw.json").write_text(json.dumps(ripe_raw, default=str))

    # ---- Step 2: rising stars ---------------------------------------------
    rising_conditions = [
        {"logical": "userCount", "operator": ">=", "value": 1_000},
        {"logical": "userCount", "operator": "<=", "value": 80_000},
        {"logical": "ratingValue", "operator": ">=", "value": 4.4},
        {"logical": "ratingCount", "operator": ">=", "value": 10},
        {"logical": "creationDate", "operator": ">=", "value": YOUNG_CUTOFF,
         "is_date": True},
    ]
    rising_raw, _ = run_search(rising_conditions, sorting="userCount",
                               label="rising")
    (STATE_DIR / "rising_raw.json").write_text(json.dumps(rising_raw, default=str))

    # ---- normalise + client-side re-filter (covers dropped conditions) ----
    seen = set()
    ripe = []
    for r in ripe_raw:
        n = normalize(r)
        if n["id"] and n["id"] not in seen and ripe_filter(n):
            seen.add(n["id"])
            ripe.append(n)
    seen = set()
    rising = []
    for r in rising_raw:
        n = normalize(r)
        if n["id"] and n["id"] not in seen and rising_filter(n):
            seen.add(n["id"])
            rising.append(n)
    print(f"candidates after filtering: ripe={len(ripe)} rising={len(rising)}")

    # ---- Step 3: scoring ---------------------------------------------------
    scored = sorted(((score(n), n) for n in ripe), key=lambda t: -t[0])

    # ---- Step 4: reviews for top 20 ----------------------------------------
    analyses = {}
    for sc, n in scored[:TOP_REVIEWS]:
        try:
            print(f"reviews: {n['name'][:50]} ({n['id']})")
            analyses[n["id"]] = analyze_reviews(n["id"])
            time.sleep(0.5)
        except BudgetExceeded:
            print("budget reached during review fetching, stopping")
            break
    (STATE_DIR / "review_analyses.json").write_text(
        json.dumps(analyses, default=str))

    # ---- Step 5: outputs ----------------------------------------------------
    ripe_rows = [to_csv_row(i + 1, n, sc) for i, (sc, n) in enumerate(scored)]
    write_csv(OUT_DIR / "ripe_candidates.csv", ripe_rows)
    rising_sorted = sorted(rising, key=lambda n: -n["users"])
    write_csv(OUT_DIR / "rising_stars.csv",
              [to_csv_row(i + 1, n) for i, n in enumerate(rising_sorted)])

    lines = ["# Chrome Web Store — Opportunity Mining Report",
             f"\n_Généré le {NOW.date().isoformat()} — "
             f"{request_count} requêtes API utilisées._\n",
             "## Top 25 « Ripe for disruption »\n"]
    for i, (sc, n) in enumerate(scored[:25], 1):
        upd = n["last_update"].date().isoformat() if n["last_update"] else "?"
        lines.append(f"### {i}. {n['name']}  (score {sc:.2f})")
        lines.append(f"- **{n['users']:,} users** · note {n['rating']:.1f} "
                     f"({n['rating_count']:,} avis) · dernière maj {upd}")
        lines.append(f"- Catégorie : {n['category']} · Dev : {n['author']}")
        lines.append(f"- [Web Store]({n['url']}) · [Chrome-Stats]({n['stats_url']})")
        an = analyses.get(n["id"])
        if an and not an.get("error"):
            top = sorted(((v, k) for k, v in an["counts"].items() if v),
                         reverse=True)[:4]
            if top:
                plaintes = ", ".join(f"{COMPLAINT_LABELS[k]} ({v})"
                                     for v, k in top)
                lines.append(f"- **Plaintes récurrentes** "
                             f"(sur {an['total_analyzed']} reviews) : {plaintes}")
            for q in an["quotes"]:
                lines.append(f"  > ★{q['rating']} — “{q['text']}”")
        lines.append("")

    lines.append("## Top 25 « Rising stars »\n")
    lines.append("| # | Extension | Users | Note | Avis | Créée | Lien |")
    lines.append("|---|-----------|-------|------|------|-------|------|")
    for i, n in enumerate(rising_sorted[:25], 1):
        created = n["created"].date().isoformat() if n["created"] else "?"
        lines.append(f"| {i} | {n['name'][:45]} | {n['users']:,} | "
                     f"{n['rating']:.1f} | {n['rating_count']:,} | {created} | "
                     f"[store]({n['url']}) |")

    (OUT_DIR / "report.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"done: {request_count} API requests, "
          f"{len(ripe)} ripe / {len(rising)} rising, report written to "
          f"{OUT_DIR / 'report.md'}")


if __name__ == "__main__":
    main()
