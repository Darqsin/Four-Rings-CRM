"""
Maricopa County Motivated Seller Lead Scraper
Scrapes the Recorder's portal for distressed property signals and enriches
with parcel / owner data from the Assessor's bulk DBF download.
"""

import asyncio
import csv
import json
import logging
import os
import re
import sys
import time
import traceback
import zipfile
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("maricopa_scraper")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
CLERK_BASE = "https://recorder.maricopa.gov/recording"
CLERK_SEARCH_URL = f"{CLERK_BASE}/document-search.html"

# Assessor bulk parcel data (CSV / DBF zip — public download)
ASSESSOR_PARCEL_URL = (
    "https://mcassessor.maricopa.gov/files/SFR_Parcel.zip"
)
ASSESSOR_FALLBACK_URLS = [
    "https://mcassessor.maricopa.gov/files/Parcel.zip",
    "https://mcassessor.maricopa.gov/files/parcel_data.zip",
]

# Doc-type categories we care about
LEAD_TYPES: dict[str, dict] = {
    # Lis Pendens / Foreclosure
    "LP":      {"cat": "LP",    "label": "Lis Pendens"},
    "NOFC":    {"cat": "FC",    "label": "Notice of Foreclosure"},
    "TAXDEED": {"cat": "TAX",   "label": "Tax Deed"},
    # Judgments
    "JUD":     {"cat": "JUD",   "label": "Judgment"},
    "CCJ":     {"cat": "JUD",   "label": "Certified Judgment"},
    "DRJUD":   {"cat": "JUD",   "label": "Domestic Judgment"},
    # Tax / Federal Liens
    "LNCORPTX":{"cat": "LIEN",  "label": "Corp Tax Lien"},
    "LNIRS":   {"cat": "LIEN",  "label": "IRS Lien"},
    "LNFED":   {"cat": "LIEN",  "label": "Federal Lien"},
    # General Liens
    "LN":      {"cat": "LIEN",  "label": "Lien"},
    "LNMECH":  {"cat": "LIEN",  "label": "Mechanic Lien"},
    "LNHOA":   {"cat": "LIEN",  "label": "HOA Lien"},
    # Medicaid
    "MEDLN":   {"cat": "LIEN",  "label": "Medicaid Lien"},
    # Probate
    "PRO":     {"cat": "PRO",   "label": "Probate Document"},
    # Notice of Commencement
    "NOC":     {"cat": "NOC",   "label": "Notice of Commencement"},
    # Release
    "RELLP":   {"cat": "REL",   "label": "Release Lis Pendens"},
}

OUTPUT_PATHS = [
    Path("dashboard/records.json"),
    Path("data/records.json"),
]
GHL_CSV_PATH = Path("data/ghl_export.csv")

# ---------------------------------------------------------------------------
# Retry helper
# ---------------------------------------------------------------------------

def retry(fn, attempts: int = 3, delay: float = 2.0):
    for i in range(attempts):
        try:
            return fn()
        except Exception as exc:
            log.warning(f"Attempt {i+1}/{attempts} failed: {exc}")
            if i < attempts - 1:
                time.sleep(delay * (i + 1))
    raise RuntimeError(f"All {attempts} attempts failed for {fn}")


# ---------------------------------------------------------------------------
# Parcel / Assessor data loader
# ---------------------------------------------------------------------------

class ParcelIndex:
    """Loads Maricopa Assessor bulk parcel data and builds owner-name lookup."""

    def __init__(self):
        self._by_owner: dict[str, list[dict]] = {}
        self._by_apn: dict[str, dict] = {}

    # ------------------------------------------------------------------
    def load(self) -> bool:
        """Download and parse parcel data. Returns True on success."""
        raw = self._download_zip()
        if raw is None:
            log.warning("Could not download parcel data — address enrichment disabled.")
            return False
        return self._parse_zip(raw)

    # ------------------------------------------------------------------
    def _download_zip(self) -> Optional[bytes]:
        urls = [ASSESSOR_PARCEL_URL] + ASSESSOR_FALLBACK_URLS
        for url in urls:
            try:
                log.info(f"Downloading parcel data from {url} …")
                resp = requests.get(url, timeout=120, stream=True)
                if resp.status_code == 200:
                    data = resp.content
                    log.info(f"Downloaded {len(data):,} bytes.")
                    return data
                log.warning(f"HTTP {resp.status_code} from {url}")
            except Exception as exc:
                log.warning(f"Failed {url}: {exc}")
        return None

    # ------------------------------------------------------------------
    def _parse_zip(self, raw: bytes) -> bool:
        try:
            zf = zipfile.ZipFile(BytesIO(raw))
        except Exception as exc:
            log.error(f"Bad zip file: {exc}")
            return False

        names = zf.namelist()
        log.info(f"Zip contents: {names}")

        # Try DBF first, fall back to CSV
        dbf_files = [n for n in names if n.lower().endswith(".dbf")]
        csv_files = [n for n in names if n.lower().endswith(".csv")]

        if dbf_files:
            return self._parse_dbf(zf, dbf_files[0])
        if csv_files:
            return self._parse_csv(zf, csv_files[0])

        log.error("No DBF or CSV found in parcel zip.")
        return False

    # ------------------------------------------------------------------
    def _parse_dbf(self, zf: zipfile.ZipFile, name: str) -> bool:
        try:
            from dbfread import DBF  # type: ignore
        except ImportError:
            log.warning("dbfread not installed — falling back to CSV if available.")
            return False

        log.info(f"Parsing DBF: {name}")
        raw = zf.read(name)
        tmp = Path("/tmp/parcel_data.dbf")
        tmp.write_bytes(raw)

        count = 0
        try:
            for rec in DBF(str(tmp), encoding="latin-1", ignore_missing_memofile=True):
                self._ingest_record(dict(rec))
                count += 1
        except Exception as exc:
            log.error(f"DBF parse error at record {count}: {exc}")

        log.info(f"Loaded {count:,} parcel records from DBF.")
        return count > 0

    # ------------------------------------------------------------------
    def _parse_csv(self, zf: zipfile.ZipFile, name: str) -> bool:
        import io
        log.info(f"Parsing CSV: {name}")
        raw = zf.read(name).decode("latin-1", errors="replace")
        reader = csv.DictReader(io.StringIO(raw))
        count = 0
        for row in reader:
            try:
                self._ingest_record(row)
                count += 1
            except Exception:
                pass
        log.info(f"Loaded {count:,} parcel records from CSV.")
        return count > 0

    # ------------------------------------------------------------------
    def _ingest_record(self, rec: dict):
        """Normalize field names and index by owner name variants and APN."""
        def g(*keys):
            for k in keys:
                v = rec.get(k, "") or rec.get(k.upper(), "") or rec.get(k.lower(), "")
                if v and str(v).strip():
                    return str(v).strip()
            return ""

        apn        = g("APN", "PARCELNO", "PARCEL_NO", "PARCEL")
        owner      = g("OWNER", "OWN1", "OWNNAME", "OWNER_NAME")
        site_addr  = g("SITE_ADDR", "SITEADDR", "SITE_ADDRESS", "SITUS_ADDR")
        site_city  = g("SITE_CITY", "SITECITY")
        site_zip   = g("SITE_ZIP",  "SITEZIP")
        mail_addr  = g("ADDR_1", "MAILADR1", "MAIL_ADDR", "MAIL_ADDRESS")
        mail_city  = g("CITY", "MAILCITY", "MAIL_CITY")
        mail_state = g("STATE", "MAILSTATE", "MAIL_STATE")
        mail_zip   = g("ZIP", "MAILZIP", "MAIL_ZIP")

        parcel = {
            "apn":        apn,
            "owner":      owner,
            "site_addr":  site_addr,
            "site_city":  site_city,
            "site_state": "AZ",
            "site_zip":   site_zip,
            "mail_addr":  mail_addr,
            "mail_city":  mail_city,
            "mail_state": mail_state or "AZ",
            "mail_zip":   mail_zip,
        }

        if apn:
            self._by_apn[apn] = parcel

        if owner:
            for variant in self._name_variants(owner):
                self._by_owner.setdefault(variant, []).append(parcel)

    # ------------------------------------------------------------------
    @staticmethod
    def _name_variants(name: str) -> list[str]:
        """Generate lookup keys: original, LAST FIRST, FIRST LAST, LAST, FIRST."""
        name = name.strip().upper()
        variants = {name}
        # If comma-separated "LAST, FIRST"
        if "," in name:
            parts = [p.strip() for p in name.split(",", 1)]
            last, first = parts[0], parts[1]
            variants.add(f"{first} {last}")   # FIRST LAST
            variants.add(f"{last} {first}")   # LAST FIRST
            variants.add(last)
        else:
            tokens = name.split()
            if len(tokens) >= 2:
                variants.add(f"{tokens[-1]} {' '.join(tokens[:-1])}")  # LAST FIRST
                variants.add(f"{tokens[-1]}, {' '.join(tokens[:-1])}")  # LAST, FIRST
        return list(variants)

    # ------------------------------------------------------------------
    def lookup(self, owner_name: str) -> Optional[dict]:
        """Find best parcel match for an owner name."""
        if not owner_name:
            return None
        upper = owner_name.strip().upper()
        for variant in self._name_variants(upper):
            hits = self._by_owner.get(variant)
            if hits:
                return hits[0]
        # Fuzzy: try substring match on first token
        first_token = upper.split()[0] if upper.split() else ""
        if len(first_token) > 3:
            for key, parcels in self._by_owner.items():
                if first_token in key:
                    return parcels[0]
        return None

    def lookup_by_apn(self, apn: str) -> Optional[dict]:
        return self._by_apn.get(apn.strip().replace("-", "").replace(" ", ""))


# ---------------------------------------------------------------------------
# Clerk Portal Scraper (Playwright async)
# ---------------------------------------------------------------------------

async def scrape_clerk(doc_type: str, start_date: str, end_date: str) -> list[dict]:
    """
    Use Playwright to search the Maricopa Recorder portal for a given
    document type code within the date range. Returns raw record dicts.
    """
    try:
        from playwright.async_api import async_playwright, TimeoutError as PWTimeout
    except ImportError:
        log.error("playwright not installed.")
        return []

    records = []
    log.info(f"Scraping clerk: {doc_type} | {start_date} → {end_date}")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
        )
        page = await ctx.new_page()

        try:
            await page.goto(CLERK_SEARCH_URL, timeout=60_000)
            await page.wait_for_load_state("networkidle", timeout=30_000)

            # ---- Fill search form ----
            # Document type field
            for sel in ["#DocType", "input[name='DocType']", "input[placeholder*='Type']"]:
                try:
                    await page.fill(sel, doc_type, timeout=3_000)
                    break
                except Exception:
                    pass

            # Date range
            for sel in ["#StartDate", "input[name='StartDate']", "input[placeholder*='Start']"]:
                try:
                    await page.fill(sel, start_date, timeout=3_000)
                    break
                except Exception:
                    pass

            for sel in ["#EndDate", "input[name='EndDate']", "input[placeholder*='End']"]:
                try:
                    await page.fill(sel, end_date, timeout=3_000)
                    break
                except Exception:
                    pass

            # Submit
            for sel in ["button[type='submit']", "input[type='submit']", "#SearchButton", ".search-btn"]:
                try:
                    await page.click(sel, timeout=3_000)
                    break
                except Exception:
                    pass

            await page.wait_for_load_state("networkidle", timeout=30_000)

            # ---- Parse results pages ----
            page_num = 1
            while True:
                html = await page.content()
                new_recs = _parse_clerk_results(html, doc_type)
                records.extend(new_recs)
                log.info(f"  {doc_type} page {page_num}: {len(new_recs)} records")

                # Next page
                next_btn = await page.query_selector(
                    "a:has-text('Next'), button:has-text('Next'), .next-page, [aria-label='Next']"
                )
                if not next_btn:
                    break
                await next_btn.click()
                await page.wait_for_load_state("networkidle", timeout=20_000)
                page_num += 1
                if page_num > 50:   # safety cap
                    break

        except PWTimeout:
            log.warning(f"Timeout scraping {doc_type} — partial results: {len(records)}")
        except Exception as exc:
            log.error(f"Error scraping {doc_type}: {exc}\n{traceback.format_exc()}")
        finally:
            await browser.close()

    return records


def _parse_clerk_results(html: str, doc_type: str) -> list[dict]:
    """Parse a single results page of the clerk portal."""
    soup = BeautifulSoup(html, "lxml")
    records = []

    # Try table rows
    tables = soup.find_all("table")
    for table in tables:
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        if not headers:
            continue

        col = {h: i for i, h in enumerate(headers)}

        for row in table.find_all("tr")[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 3:
                continue

            def cell(key, fallback=""):
                idx = col.get(key)
                if idx is not None and idx < len(cells):
                    return cells[idx].get_text(strip=True)
                return fallback

            # Extract link
            link = ""
            for a in row.find_all("a", href=True):
                href = a["href"]
                if href.startswith("http"):
                    link = href
                elif href.startswith("/"):
                    link = f"https://recorder.maricopa.gov{href}"
                break

            # Try to find doc number in first cell or a link text
            doc_num = (
                cell("doc #") or cell("document #") or cell("document number")
                or cell("doc number") or cell("instrument")
                or (cells[0].get_text(strip=True) if cells else "")
            )

            filed = (
                cell("date filed") or cell("filed date") or cell("recorded date")
                or cell("date") or cell("recording date")
            )

            grantor = (
                cell("grantor") or cell("owner") or cell("party 1") or cell("name")
            )

            grantee = (
                cell("grantee") or cell("party 2") or cell("lender") or cell("payee")
            )

            amount_raw = (
                cell("amount") or cell("consideration") or cell("debt") or ""
            )

            legal = (
                cell("legal description") or cell("legal") or cell("description") or ""
            )

            dtype = (
                cell("doc type") or cell("document type") or cell("type") or doc_type
            )

            if not doc_num or doc_num.lower() in ("doc #", "document #", ""):
                continue

            records.append({
                "doc_num":  doc_num,
                "doc_type": dtype or doc_type,
                "filed":    _normalize_date(filed),
                "grantor":  grantor,
                "grantee":  grantee,
                "amount":   _parse_amount(amount_raw),
                "legal":    legal,
                "clerk_url": link,
            })

    # If no tables found, try JSON embedded in script tags
    if not records:
        for script in soup.find_all("script"):
            txt = script.string or ""
            if "docNum" in txt or "DocNumber" in txt or "recorded" in txt.lower():
                try:
                    m = re.search(r"\[.*?\]", txt, re.DOTALL)
                    if m:
                        items = json.loads(m.group())
                        for item in items:
                            if isinstance(item, dict):
                                records.append(_normalize_json_rec(item, doc_type))
                except Exception:
                    pass

    return records


def _normalize_json_rec(item: dict, default_type: str) -> dict:
    def g(*keys):
        for k in keys:
            for variant in (k, k.lower(), k.upper()):
                if v := item.get(variant):
                    return str(v).strip()
        return ""

    return {
        "doc_num":  g("docNum", "DocNumber", "InstrumentNumber"),
        "doc_type": g("docType", "DocType", "type") or default_type,
        "filed":    _normalize_date(g("filedDate", "RecordedDate", "date")),
        "grantor":  g("grantor", "Grantor", "owner", "Owner"),
        "grantee":  g("grantee", "Grantee"),
        "amount":   _parse_amount(g("amount", "Amount", "consideration")),
        "legal":    g("legal", "LegalDescription"),
        "clerk_url": g("url", "link", "URL"),
    }


def _normalize_date(raw: str) -> str:
    if not raw:
        return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%m/%d/%y", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw.strip()


def _parse_amount(raw: str) -> Optional[float]:
    if not raw:
        return None
    clean = re.sub(r"[^\d.]", "", raw)
    try:
        return float(clean) if clean else None
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Seller Score
# ---------------------------------------------------------------------------

def compute_flags(rec: dict, all_records: list[dict]) -> tuple[list[str], int]:
    flags = []
    score = 30  # base

    cat   = rec.get("cat", "")
    owner = (rec.get("owner") or "").upper()
    amt   = rec.get("amount") or 0
    filed = rec.get("filed", "")

    # Category-based flags
    if cat == "LP":
        flags.append("Lis pendens")
        score += 10
    if cat == "FC":
        flags.append("Pre-foreclosure")
        score += 10
    if cat == "JUD":
        flags.append("Judgment lien")
        score += 10
    if cat in ("TAX", "LIEN") and rec.get("doc_type", "").startswith(("LNCORP", "LNIRS", "LNFED", "TAX")):
        flags.append("Tax lien")
        score += 10
    if cat == "LIEN" and rec.get("doc_type") == "LNMECH":
        flags.append("Mechanic lien")
        score += 10
    if cat == "PRO":
        flags.append("Probate / estate")
        score += 10
    if cat == "LIEN" and rec.get("doc_type") == "LNHOA":
        flags.append("HOA lien")
        score += 5

    # LP + Foreclosure combo
    doc_num = rec.get("doc_num", "")
    owner_lp = any(
        r.get("cat") == "LP" and r.get("owner", "").upper() == owner
        for r in all_records if r.get("doc_num") != doc_num
    )
    owner_fc = any(
        r.get("cat") == "FC" and r.get("owner", "").upper() == owner
        for r in all_records if r.get("doc_num") != doc_num
    )
    if (cat in ("LP", "FC")) and (owner_lp or owner_fc):
        score += 20

    # Amount flags
    if amt and amt > 100_000:
        score += 15
        if "High-value debt" not in flags:
            flags.append("High-value debt")
    elif amt and amt > 50_000:
        score += 10

    # New this week
    try:
        d = datetime.strptime(filed, "%Y-%m-%d")
        if (datetime.now() - d).days <= 7:
            score += 5
            flags.append("New this week")
    except Exception:
        pass

    # Has address
    if rec.get("prop_address") or rec.get("mail_address"):
        score += 5

    # LLC / Corp owner
    if any(kw in owner for kw in ("LLC", "INC", "CORP", "LP ", "LTD", "TRUST", "HOLDINGS")):
        flags.append("LLC / corp owner")

    score = min(score, 100)
    return flags, score


# ---------------------------------------------------------------------------
# GHL CSV export
# ---------------------------------------------------------------------------

GHL_COLUMNS = [
    "First Name", "Last Name", "Mailing Address", "Mailing City",
    "Mailing State", "Mailing Zip", "Property Address", "Property City",
    "Property State", "Property Zip", "Lead Type", "Document Type",
    "Date Filed", "Document Number", "Amount/Debt Owed",
    "Seller Score", "Motivated Seller Flags", "Source", "Public Records URL",
]


def _split_name(full: str) -> tuple[str, str]:
    """Split 'FIRST LAST' or 'LAST, FIRST' into (first, last)."""
    if not full:
        return "", ""
    if "," in full:
        parts = full.split(",", 1)
        return parts[1].strip().title(), parts[0].strip().title()
    tokens = full.strip().split()
    if len(tokens) == 1:
        return "", tokens[0].title()
    return tokens[0].title(), " ".join(tokens[1:]).title()


def write_ghl_csv(records: list[dict], path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=GHL_COLUMNS)
        w.writeheader()
        for r in records:
            first, last = _split_name(r.get("owner") or "")
            w.writerow({
                "First Name":           first,
                "Last Name":            last,
                "Mailing Address":      r.get("mail_address", ""),
                "Mailing City":         r.get("mail_city", ""),
                "Mailing State":        r.get("mail_state", ""),
                "Mailing Zip":          r.get("mail_zip", ""),
                "Property Address":     r.get("prop_address", ""),
                "Property City":        r.get("prop_city", ""),
                "Property State":       r.get("prop_state", "AZ"),
                "Property Zip":         r.get("prop_zip", ""),
                "Lead Type":            r.get("cat_label", ""),
                "Document Type":        r.get("doc_type", ""),
                "Date Filed":           r.get("filed", ""),
                "Document Number":      r.get("doc_num", ""),
                "Amount/Debt Owed":     r.get("amount", ""),
                "Seller Score":         r.get("score", ""),
                "Motivated Seller Flags": "; ".join(r.get("flags", [])),
                "Source":               "Maricopa County Recorder",
                "Public Records URL":   r.get("clerk_url", ""),
            })
    log.info(f"GHL CSV → {path} ({len(records)} rows)")


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

async def main():
    end_dt   = datetime.now()
    start_dt = end_dt - timedelta(days=LOOKBACK_DAYS)
    start_str = start_dt.strftime("%m/%d/%Y")
    end_str   = end_dt.strftime("%m/%d/%Y")

    log.info(f"=== Maricopa Motivated Seller Scraper ===")
    log.info(f"Date range: {start_str} → {end_str}")
    log.info(f"Lead types: {list(LEAD_TYPES.keys())}")

    # 1. Load parcel data
    parcel_index = ParcelIndex()
    parcel_loaded = parcel_index.load()

    # 2. Scrape each doc type
    all_raw: list[dict] = []
    for code in LEAD_TYPES:
        try:
            recs = await scrape_clerk(code, start_str, end_str)
            for r in recs:
                r["_code"] = code
            all_raw.extend(recs)
            log.info(f"  ✓ {code}: {len(recs)} records")
        except Exception as exc:
            log.error(f"  ✗ {code} failed: {exc}")

    log.info(f"Total raw records: {len(all_raw)}")

    # 3. Deduplicate by doc_num
    seen = set()
    deduped = []
    for r in all_raw:
        key = r.get("doc_num", "")
        if key and key not in seen:
            seen.add(key)
            deduped.append(r)
        elif not key:
            deduped.append(r)

    log.info(f"After dedup: {len(deduped)}")

    # 4. Enrich each record
    enriched = []
    for raw in deduped:
        code  = raw.get("_code", raw.get("doc_type", ""))
        meta  = LEAD_TYPES.get(code, {"cat": "OTHER", "label": code})

        # Parcel enrichment
        parcel = None
        if parcel_loaded:
            parcel = parcel_index.lookup(raw.get("grantor", ""))
            if not parcel:
                parcel = parcel_index.lookup(raw.get("grantee", ""))

        rec = {
            "doc_num":      raw.get("doc_num", ""),
            "doc_type":     raw.get("doc_type", code),
            "filed":        raw.get("filed", ""),
            "cat":          meta["cat"],
            "cat_label":    meta["label"],
            "owner":        raw.get("grantor", ""),
            "grantee":      raw.get("grantee", ""),
            "amount":       raw.get("amount"),
            "legal":        raw.get("legal", ""),
            "prop_address": (parcel or {}).get("site_addr", ""),
            "prop_city":    (parcel or {}).get("site_city", ""),
            "prop_state":   (parcel or {}).get("site_state", "AZ"),
            "prop_zip":     (parcel or {}).get("site_zip", ""),
            "mail_address": (parcel or {}).get("mail_addr", ""),
            "mail_city":    (parcel or {}).get("mail_city", ""),
            "mail_state":   (parcel or {}).get("mail_state", "AZ"),
            "mail_zip":     (parcel or {}).get("mail_zip", ""),
            "clerk_url":    raw.get("clerk_url", ""),
            "flags":        [],
            "score":        30,
        }
        enriched.append(rec)

    # 5. Compute scores (needs full list for combo detection)
    for rec in enriched:
        try:
            flags, score = compute_flags(rec, enriched)
            rec["flags"] = flags
            rec["score"] = score
        except Exception as exc:
            log.warning(f"Score error for {rec.get('doc_num')}: {exc}")

    # Sort by score descending
    enriched.sort(key=lambda r: r.get("score", 0), reverse=True)

    # 6. Build output payload
    with_address = sum(1 for r in enriched if r.get("prop_address") or r.get("mail_address"))

    payload = {
        "fetched_at":    datetime.utcnow().isoformat() + "Z",
        "source":        "Maricopa County Recorder",
        "date_range":    {"start": start_str, "end": end_str},
        "total":         len(enriched),
        "with_address":  with_address,
        "records":       enriched,
    }

    # 7. Save JSON outputs
    for path in OUTPUT_PATHS:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, indent=2, default=str))
            log.info(f"Saved → {path}")
        except Exception as exc:
            log.error(f"Failed saving {path}: {exc}")

    # 8. GHL CSV export
    try:
        write_ghl_csv(enriched, GHL_CSV_PATH)
    except Exception as exc:
        log.error(f"GHL CSV failed: {exc}")

    log.info(
        f"\n=== DONE ===\n"
        f"  Total records : {len(enriched)}\n"
        f"  With address  : {with_address}\n"
        f"  Parcel data   : {'✓ loaded' if parcel_loaded else '✗ unavailable'}\n"
    )


if __name__ == "__main__":
    asyncio.run(main())
