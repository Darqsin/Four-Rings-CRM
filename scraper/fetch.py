"""
Maricopa County Motivated Seller Lead Scraper  v2.0
====================================================
Combines:
  1. Recorder portal search (Playwright) — all motivated-seller doc types
  2. Notice of Trustee Sale (NOTS) PDF pipeline — download & parse every PDF
     with multi-strategy field extraction + OCR fallback for scanned PDFs.
  3. Assessor bulk parcel enrichment (DBF/CSV zip) — owner / address lookup.
  4. Seller scoring (0-100) with flag system.
  5. Output: dashboard/records.json + data/records.json
             + data/leads.xlsx  (3 sheets: All Leads, NOTS Only, Summary)
             + data/ghl_export.csv
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import re
import sys
import tempfile
import time
import traceback
import zipfile
from datetime import datetime, timedelta
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
log = logging.getLogger("maricopa")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
LOOKBACK_DAYS  = int(os.getenv("LOOKBACK_DAYS", "7"))
CLERK_BASE     = "https://recorder.maricopa.gov"
CLERK_SEARCH   = f"{CLERK_BASE}/recording/document-search.html"

ASSESSOR_URLS = [
    "https://mcassessor.maricopa.gov/files/SFR_Parcel.zip",
    "https://mcassessor.maricopa.gov/files/Parcel.zip",
    "https://mcassessor.maricopa.gov/files/parcel_data.zip",
    "https://mcassessor.maricopa.gov/files/ResidentialParcel.zip",
]

OUTPUT_PATHS = [Path("dashboard/records.json"), Path("data/records.json")]
XLSX_PATH    = Path("data/leads.xlsx")
GHL_CSV_PATH = Path("data/ghl_export.csv")

LEAD_TYPES: dict[str, dict] = {
    "LP":       {"cat": "LP",   "label": "Lis Pendens"},
    "NOFC":     {"cat": "FC",   "label": "Notice of Foreclosure"},
    "NOTS":     {"cat": "NOTS", "label": "Notice of Trustee Sale"},
    "TAXDEED":  {"cat": "TAX",  "label": "Tax Deed"},
    "JUD":      {"cat": "JUD",  "label": "Judgment"},
    "CCJ":      {"cat": "JUD",  "label": "Certified Judgment"},
    "DRJUD":    {"cat": "JUD",  "label": "Domestic Judgment"},
    "LNCORPTX": {"cat": "LIEN", "label": "Corp Tax Lien"},
    "LNIRS":    {"cat": "LIEN", "label": "IRS Lien"},
    "LNFED":    {"cat": "LIEN", "label": "Federal Lien"},
    "LN":       {"cat": "LIEN", "label": "Lien"},
    "LNMECH":   {"cat": "LIEN", "label": "Mechanic Lien"},
    "LNHOA":    {"cat": "LIEN", "label": "HOA Lien"},
    "MEDLN":    {"cat": "LIEN", "label": "Medicaid Lien"},
    "PRO":      {"cat": "PRO",  "label": "Probate Document"},
    "NOC":      {"cat": "NOC",  "label": "Notice of Commencement"},
    "RELLP":    {"cat": "REL",  "label": "Release Lis Pendens"},
}


# ===========================================================================
# Helpers
# ===========================================================================

def _norm_date(raw: str) -> str:
    if not raw:
        return ""
    raw = raw.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%m/%d/%y",
                "%d/%m/%Y", "%B %d, %Y", "%b %d, %Y", "%d %B %Y",
                "%B %d %Y", "%b %d %Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    m = re.search(r'(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})', raw)
    if m:
        mo, dy, yr = m.group(1), m.group(2), m.group(3)
        if len(yr) == 2:
            yr = "20" + yr
        try:
            return datetime.strptime(f"{mo}/{dy}/{yr}", "%m/%d/%Y").strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw


def _parse_amount(raw) -> Optional[float]:
    if raw is None:
        return None
    clean = re.sub(r"[^\d.]", "", str(raw))
    try:
        v = float(clean)
        return v if v > 0 else None
    except ValueError:
        return None


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r'\s+', ' ', str(s)).strip()


# ===========================================================================
# PDF Parser — Notice of Trustee Sale
# ===========================================================================

class NOTSPdfParser:
    """
    Download a Maricopa Recorder PDF and extract NOTS fields using a
    cascade of strategies:
      1. pdfplumber  (best for text-layer PDFs)
      2. pdfminer    (fallback text-layer)
      3. PyMuPDF + pytesseract OCR  (scanned / image PDFs)
    """

    _PAT: dict[str, list[re.Pattern]] = {
        "trustor": [
            re.compile(r'(?:Trustor|Grantor|Owner)[s\s]*:?\s*([A-Z][^\n,;]{3,80})', re.I),
            re.compile(r'(?:herein\s+(?:called|referred\s+to\s+as)\s+(?:the\s+)?(?:Trustor|Grantor))[,\s]+([A-Z][^\n,;]{3,80})', re.I),
        ],
        "beneficiary": [
            re.compile(r'(?:Beneficiar(?:y|ies)|Lender|Mortgagee)[s\s]*:?\s*([^\n,;]{3,100})', re.I),
            re.compile(r'(?:for\s+the\s+benefit\s+of)[:\s]+([^\n,;]{3,100})', re.I),
        ],
        "amount": [
            re.compile(r'(?:unpaid\s+(?:principal\s+)?balance|total\s+(?:amount\s+of\s+)?(?:unpaid\s+)?(?:indebtedness|debt|obligation)|amount\s+of\s+unpaid\s+debt)[^\$\d]*\$?\s*([\d,]+(?:\.\d{1,2})?)', re.I),
            re.compile(r'(?:sum\s+of|total\s+debt|amount\s+owed|balance\s+due|principal\s+sum)[^\$\d]*\$?\s*([\d,]+(?:\.\d{1,2})?)', re.I),
        ],
        "sale_date": [
            re.compile(r'(?:sale\s+(?:will\s+be\s+)?(?:held|conducted|occur|take\s+place)|date\s+of\s+(?:trustee.s\s+)?sale)[^\d]*(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})', re.I),
            re.compile(r'on\s+(\w+\s+\d{1,2},?\s*\d{4})\s+at\s+\d', re.I),
            re.compile(r'(?:sale\s+date)[:\s]+(\w+\s+\d{1,2},?\s*\d{4})', re.I),
        ],
        "sale_location": [
            re.compile(r'(?:at\s+|location[:\s]+)(.{10,140}?(?:Street|Ave|Blvd|Drive|Court|Way|Rd|Courthouse|Building)[^\n]*)', re.I),
        ],
        "property_address": [
            re.compile(r'(?:property\s+(?:address|commonly\s+known\s+as|located\s+at)|street\s+address|premises\s+(?:known\s+as|located\s+at)|commonly\s+known\s+as)[:\s]+([^\n]{5,140})', re.I),
            re.compile(r'(\d{2,6}\s+\w[^\n]{5,80}(?:Street|St\.?|Ave\.?|Blvd\.?|Drive|Dr\.?|Court|Ct\.?|Way|Place|Pl\.?|Lane|Ln\.?|Circle|Cir\.?|Road|Rd\.?)\b[^\n]{0,40})', re.I),
        ],
        "apn": [
            re.compile(r'(?:APN|Assessor.?s?\s+Parcel\s+(?:No\.?|Number)|Parcel\s+(?:No\.?|Number)|Tax\s+(?:ID|Parcel))[:\s#]+(\d{3}[\-\s]?\d{2}[\-\s]?\d{3,4})', re.I),
        ],
        "legal": [
            re.compile(r'(?:legal\s+description|described\s+as\s+follows?)[:\s]+(.{20,500}?)(?:\n{2,}|APN|Trustor|Trustee\s+Sale|Sale\s+Date|\Z)', re.I | re.S),
        ],
        "doc_number": [
            re.compile(r'(?:instrument\s+(?:no\.?|number)|document\s+(?:no\.?|number)|recording\s+(?:no\.?|number)|doc\.?\s*(?:no\.?|#))[:\s#]*(\d{7,12})', re.I),
        ],
    }

    def __init__(self, session: requests.Session):
        self._session = session

    def fetch_and_parse(self, url: str, doc_num: str = "") -> dict:
        result: dict = {
            "trustor": "", "beneficiary": "", "amount": "",
            "sale_date": "", "sale_location": "", "property_address": "",
            "apn": "", "legal": "", "doc_number": doc_num,
            "_pdf_ok": False, "_ocr_used": False,
        }
        try:
            pdf_bytes = self._download(url)
            if not pdf_bytes:
                return result

            text = self._text_pdfplumber(pdf_bytes)
            if len(text.strip()) < 80:
                text = self._text_pdfminer(pdf_bytes)
            if len(text.strip()) < 80:
                text = self._text_ocr(pdf_bytes)
                result["_ocr_used"] = True

            if len(text.strip()) > 30:
                result["_pdf_ok"] = True
                self._match(text, result)
                result["amount"]  = self._best_amount(text, result["amount"])
                result["trustor"] = self._clean_name(result["trustor"])
                result["apn"]     = self._clean_apn(result["apn"])

        except Exception as exc:
            log.warning(f"PDF error ({url}): {exc}")
        return result

    # ── Download ─────────────────────────────────────────────────────────────

    def _download(self, url: str) -> Optional[bytes]:
        for attempt in range(3):
            try:
                r = self._session.get(url, timeout=30, stream=True)
                if r.status_code == 200:
                    return r.content
                log.debug(f"PDF HTTP {r.status_code}: {url}")
            except Exception as exc:
                log.debug(f"PDF download attempt {attempt+1}: {exc}")
                time.sleep(2 ** attempt)
        return None

    # ── Text extraction ───────────────────────────────────────────────────────

    @staticmethod
    def _text_pdfplumber(pdf_bytes: bytes) -> str:
        try:
            import pdfplumber
            parts = []
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages[:8]:
                    t = page.extract_text(x_tolerance=2, y_tolerance=2) or ""
                    parts.append(t)
                    try:
                        for tbl in (page.extract_tables() or []):
                            for row in tbl:
                                if row:
                                    parts.append(" | ".join(str(c or "") for c in row))
                    except Exception:
                        pass
            return "\n".join(parts)
        except Exception:
            return ""

    @staticmethod
    def _text_pdfminer(pdf_bytes: bytes) -> str:
        try:
            from pdfminer.high_level import extract_text
            return extract_text(io.BytesIO(pdf_bytes)) or ""
        except Exception:
            return ""

    @staticmethod
    def _text_ocr(pdf_bytes: bytes) -> str:
        parts = []
        # Strategy A: PyMuPDF render + pytesseract
        try:
            import fitz
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            for i in range(min(len(doc), 6)):
                pix = doc[i].get_pixmap(matrix=fitz.Matrix(2.5, 2.5))
                parts.append(NOTSPdfParser._ocr_bytes(pix.tobytes("png")))
            doc.close()
            if any(parts):
                return "\n".join(parts)
        except ImportError:
            pass
        except Exception as exc:
            log.debug(f"PyMuPDF OCR failed: {exc}")

        # Strategy B: pdf2image + pytesseract
        try:
            from pdf2image import convert_from_bytes
            imgs = convert_from_bytes(pdf_bytes, dpi=250, first_page=1, last_page=6)
            for img in imgs:
                buf = io.BytesIO()
                img.save(buf, "PNG")
                parts.append(NOTSPdfParser._ocr_bytes(buf.getvalue()))
        except ImportError:
            pass
        except Exception as exc:
            log.debug(f"pdf2image OCR failed: {exc}")

        return "\n".join(parts)

    @staticmethod
    def _ocr_bytes(img_bytes: bytes) -> str:
        try:
            import pytesseract
            from PIL import Image
            return pytesseract.image_to_string(
                Image.open(io.BytesIO(img_bytes)),
                config="--oem 3 --psm 6"
            ) or ""
        except Exception:
            return ""

    # ── Pattern matching ──────────────────────────────────────────────────────

    def _match(self, text: str, result: dict):
        for field, patterns in self._PAT.items():
            if result.get(field):
                continue
            for pat in patterns:
                m = pat.search(text)
                if m:
                    val = _clean(m.group(1) if m.lastindex else m.group())
                    if val and len(val) > 2:
                        result[field] = val
                        break

    @staticmethod
    def _best_amount(text: str, current: str) -> str:
        """Pick the largest plausible dollar amount as the loan balance."""
        amounts = []
        for a in re.findall(r'\$\s*([\d,]+(?:\.\d{1,2})?)', text):
            try:
                v = float(a.replace(",", ""))
                if 5_000 < v < 50_000_000:
                    amounts.append(v)
            except ValueError:
                pass
        if not amounts:
            return current
        best = max(amounts)
        if current:
            try:
                cur_v = float(re.sub(r'[^\d.]', '', current))
                if cur_v > best:
                    return current
            except ValueError:
                pass
        return f"{best:,.2f}"

    @staticmethod
    def _clean_name(name: str) -> str:
        if not name:
            return ""
        stops = [
            r',?\s+(?:a|an)\s+(?:married|single|unmarried|Arizona)',
            r',?\s+(?:husband\s+and\s+wife|joint\s+tenants?|community\s+property)',
            r',?\s+as\s+(?:trustee|beneficiary|grantor|his|her)',
            r'\s+herein\s+.*',
            r'\s+\(herein.*',
        ]
        for s in stops:
            name = re.sub(s, "", name, flags=re.I)
        return _clean(name)

    @staticmethod
    def _clean_apn(apn: str) -> str:
        if not apn:
            return ""
        d = re.sub(r'\D', '', apn)
        if len(d) == 8:
            return f"{d[:3]}-{d[3:5]}-{d[5:]}"
        if len(d) == 9:
            return f"{d[:3]}-{d[3:5]}-{d[5:]}"
        return apn


# ===========================================================================
# Parcel Index
# ===========================================================================

class ParcelIndex:
    def __init__(self):
        self._by_owner: dict[str, list[dict]] = {}
        self._by_apn:   dict[str, dict]       = {}

    def load(self) -> bool:
        for url in ASSESSOR_URLS:
            try:
                log.info(f"Downloading parcel data: {url}")
                resp = requests.get(url, timeout=120, stream=True)
                if resp.status_code != 200:
                    log.warning(f"HTTP {resp.status_code}: {url}")
                    continue
                raw = resp.content
                log.info(f"Downloaded {len(raw):,} bytes")
                if self._parse_zip(raw):
                    return True
            except Exception as exc:
                log.warning(f"Parcel download failed ({url}): {exc}")
        log.warning("Parcel data unavailable — address enrichment disabled")
        return False

    def _parse_zip(self, raw: bytes) -> bool:
        try:
            zf = zipfile.ZipFile(io.BytesIO(raw))
        except Exception as exc:
            log.error(f"Bad zip: {exc}")
            return False
        names = zf.namelist()
        log.info(f"Zip contents: {names}")
        dbfs = [n for n in names if n.lower().endswith(".dbf")]
        csvs = [n for n in names if n.lower().endswith(".csv")]
        if dbfs:
            return self._parse_dbf(zf, dbfs[0])
        if csvs:
            return self._parse_csv(zf, csvs[0])
        log.error("No DBF or CSV in parcel zip")
        return False

    def _parse_dbf(self, zf: zipfile.ZipFile, name: str) -> bool:
        try:
            from dbfread import DBF
        except ImportError:
            log.warning("dbfread not installed — trying CSV")
            return False
        raw = zf.read(name)
        tmp = Path(tempfile.mktemp(suffix=".dbf"))
        tmp.write_bytes(raw)
        count = 0
        try:
            for rec in DBF(str(tmp), encoding="latin-1", ignore_missing_memofile=True):
                self._ingest(dict(rec))
                count += 1
        except Exception as exc:
            log.error(f"DBF error at record {count}: {exc}")
        finally:
            tmp.unlink(missing_ok=True)
        log.info(f"Loaded {count:,} parcel records (DBF)")
        return count > 0

    def _parse_csv(self, zf: zipfile.ZipFile, name: str) -> bool:
        raw = zf.read(name).decode("latin-1", errors="replace")
        reader = csv.DictReader(io.StringIO(raw))
        count = 0
        for row in reader:
            try:
                self._ingest(row)
                count += 1
            except Exception:
                pass
        log.info(f"Loaded {count:,} parcel records (CSV)")
        return count > 0

    def _ingest(self, rec: dict):
        def g(*keys):
            for k in keys:
                for v in (k, k.upper(), k.lower()):
                    val = rec.get(v, "")
                    if val and str(val).strip() not in ("", "None"):
                        return str(val).strip()
            return ""

        apn       = g("APN", "PARCELNO", "PARCEL_NO", "PARCEL")
        owner     = g("OWNER", "OWN1", "OWNNAME", "OWNER_NAME", "OWNER1")
        site_addr = g("SITE_ADDR", "SITEADDR", "SITE_ADDRESS", "SITUS_ADDR")
        site_city = g("SITE_CITY", "SITECITY", "SCITY")
        site_zip  = g("SITE_ZIP", "SITEZIP", "SZIP")
        mail_addr = g("ADDR_1", "MAILADR1", "MAIL_ADDR", "MAIL_ADDRESS", "MAILADD")
        mail_city = g("CITY", "MAILCITY", "MAIL_CITY", "MCITY")
        mail_st   = g("STATE", "MAILSTATE", "MAIL_STATE", "MSTATE")
        mail_zip  = g("ZIP", "MAILZIP", "MAIL_ZIP", "MZIP")

        parcel = {
            "apn": self._norm_apn(apn), "owner": owner,
            "site_addr": site_addr, "site_city": site_city,
            "site_state": "AZ",     "site_zip": site_zip,
            "mail_addr": mail_addr, "mail_city": mail_city,
            "mail_state": mail_st or "AZ", "mail_zip": mail_zip,
        }
        norm = self._norm_apn(apn)
        if norm:
            self._by_apn[norm] = parcel
        if owner:
            for v in self._name_variants(owner):
                self._by_owner.setdefault(v, []).append(parcel)

    @staticmethod
    def _norm_apn(apn: str) -> str:
        return re.sub(r'\D', '', apn or "")

    @staticmethod
    def _name_variants(name: str) -> list[str]:
        name = name.strip().upper()
        variants = {name}
        if "," in name:
            parts = [p.strip() for p in name.split(",", 1)]
            last, first = parts[0], parts[1]
            variants.update({f"{first} {last}", f"{last} {first}", last})
        else:
            toks = name.split()
            if len(toks) >= 2:
                variants.update({
                    f"{toks[-1]} {' '.join(toks[:-1])}",
                    f"{toks[-1]}, {' '.join(toks[:-1])}",
                })
        return list(variants)

    def lookup(self, owner_name: str) -> Optional[dict]:
        if not owner_name:
            return None
        upper = owner_name.strip().upper()
        for v in self._name_variants(upper):
            hits = self._by_owner.get(v)
            if hits:
                return hits[0]
        # Fuzzy token match
        for tok in [t for t in upper.split() if len(t) > 3][:2]:
            for key, parcels in self._by_owner.items():
                if tok in key:
                    return parcels[0]
        return None

    def lookup_apn(self, apn: str) -> Optional[dict]:
        norm = self._norm_apn(apn)
        return self._by_apn.get(norm) if norm else None


# ===========================================================================
# Portal Scraper (Playwright)
# ===========================================================================

async def scrape_clerk(code: str, start_str: str, end_str: str) -> list[dict]:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.error("playwright not installed")
        return []

    records: list[dict] = []
    log.info(f"Scraping portal: {code} | {start_str} → {end_str}")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        page = await ctx.new_page()

        try:
            await page.goto(CLERK_SEARCH, timeout=60_000, wait_until="networkidle")

            for sel in ["#DocType", "input[name='DocType']", "input[placeholder*='Type']"]:
                try:
                    await page.fill(sel, code, timeout=3_000); break
                except Exception:
                    pass
            for sel in ["#StartDate", "input[name='StartDate']", "input[placeholder*='Start']"]:
                try:
                    await page.fill(sel, start_str, timeout=3_000); break
                except Exception:
                    pass
            for sel in ["#EndDate", "input[name='EndDate']", "input[placeholder*='End']"]:
                try:
                    await page.fill(sel, end_str, timeout=3_000); break
                except Exception:
                    pass
            for sel in ["button[type='submit']", "input[type='submit']",
                        "#SearchButton", "button:has-text('Search')"]:
                try:
                    await page.click(sel, timeout=3_000); break
                except Exception:
                    pass

            await page.wait_for_load_state("networkidle", timeout=30_000)

            page_num = 1
            while page_num <= 50:
                html  = await page.content()
                batch = _parse_page(html, code)
                records.extend(batch)
                log.info(f"  {code} pg {page_num}: {len(batch)}")

                next_btn = await page.query_selector(
                    "a:has-text('Next'), button:has-text('Next'), "
                    ".next-page, .pagination-next"
                )
                if not next_btn:
                    break
                try:
                    await next_btn.click(timeout=5_000)
                    await page.wait_for_load_state("networkidle", timeout=20_000)
                    page_num += 1
                except Exception:
                    break

        except Exception as exc:
            log.error(f"Portal error ({code}): {exc}")
        finally:
            await browser.close()

    return records


def _parse_page(html: str, code: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    records: list[dict] = []

    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        if not headers:
            continue
        col = {h: i for i, h in enumerate(headers)}

        for row in table.find_all("tr")[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            def cell(*keys, fallback=""):
                for k in keys:
                    idx = col.get(k)
                    if idx is not None and idx < len(cells):
                        return cells[idx].get_text(strip=True)
                return fallback

            link = ""
            for a in row.find_all("a", href=True):
                href = a["href"]
                if href.startswith("http"):
                    link = href
                elif href.startswith("/"):
                    link = CLERK_BASE + href
                if link:
                    break

            doc_num = _clean(
                cell("doc #", "document #", "instrument #", "doc number",
                     "document number", "instrument number")
                or cells[0].get_text(strip=True)
            )
            if not doc_num or doc_num.lower() in ("doc #", "document #", ""):
                continue

            records.append({
                "doc_num":  doc_num,
                "doc_type": _clean(cell("doc type", "document type", "type") or code),
                "filed":    _norm_date(cell("date filed", "filed date", "recorded date",
                                            "recording date", "date")),
                "grantor":  _clean(cell("grantor", "owner", "name", "party 1")),
                "grantee":  _clean(cell("grantee", "lender", "party 2")),
                "amount":   _parse_amount(cell("amount", "consideration", "debt")),
                "legal":    _clean(cell("legal description", "legal", "description")),
                "clerk_url": link,
                "_code":    code,
            })

    # JSON fallback
    if not records:
        for script in soup.find_all("script"):
            txt = script.string or ""
            if "DocNumber" in txt or "InstrumentNumber" in txt:
                try:
                    m = re.search(r'\[.*?\]', txt, re.DOTALL)
                    if m:
                        for item in json.loads(m.group()):
                            if isinstance(item, dict):
                                records.append(_json_rec(item, code))
                except Exception:
                    pass

    return records


def _json_rec(item: dict, code: str) -> dict:
    def g(*keys):
        for k in keys:
            for var in (k, k.lower(), k.upper()):
                v = item.get(var)
                if v:
                    return str(v).strip()
        return ""
    return {
        "doc_num":   g("docNum", "DocNumber", "InstrumentNumber"),
        "doc_type":  g("docType", "DocType") or code,
        "filed":     _norm_date(g("filedDate", "RecordedDate", "DateRecorded")),
        "grantor":   _clean(g("grantor", "Grantor", "owner", "Owner")),
        "grantee":   _clean(g("grantee", "Grantee")),
        "amount":    _parse_amount(g("amount", "Amount", "consideration")),
        "legal":     _clean(g("legal", "LegalDescription")),
        "clerk_url": g("url", "URL", "link"),
        "_code":     code,
    }


# ===========================================================================
# NOTS PDF Pipeline
# ===========================================================================

async def run_nots_pipeline(
    records: list[dict],
    parcels: ParcelIndex,
    session: requests.Session,
) -> list[dict]:
    parser = NOTSPdfParser(session)
    nots  = [r for r in records if r.get("cat") == "NOTS" and r.get("clerk_url")]
    other = [r for r in records if not (r.get("cat") == "NOTS" and r.get("clerk_url"))]

    log.info(f"NOTS PDF pipeline: {len(nots)} records")
    enriched_nots: list[dict] = []

    for rec in nots:
        url = rec.get("clerk_url", "")
        try:
            pdf = await asyncio.get_event_loop().run_in_executor(
                None, parser.fetch_and_parse, url, rec.get("doc_num", "")
            )
        except Exception as exc:
            log.warning(f"PDF executor error ({url}): {exc}")
            pdf = {}

        merged = dict(rec)
        # Owner: PDF trustor wins (more reliable than portal scrape)
        if pdf.get("trustor"):
            merged["owner"] = pdf["trustor"]
        if pdf.get("beneficiary"):
            merged["grantee"] = pdf["beneficiary"]
        # Amount: PDF loan balance wins
        if pdf.get("amount"):
            v = _parse_amount(pdf["amount"])
            if v:
                merged["amount"] = v
        # Extra NOTS fields
        for fld in ("sale_date", "sale_location", "legal"):
            if pdf.get(fld) and not merged.get(fld):
                merged[fld] = pdf[fld][:300] if fld == "legal" else pdf[fld]

        # Address: APN lookup wins, then name lookup, then PDF address
        parcel = None
        if pdf.get("apn"):
            parcel = parcels.lookup_apn(pdf["apn"])
        if not parcel:
            parcel = parcels.lookup(merged.get("owner", ""))
        _apply_parcel(merged, parcel, pdf.get("property_address", ""))

        merged["_pdf_apn"]  = pdf.get("apn", "")
        merged["_pdf_ok"]   = pdf.get("_pdf_ok", False)
        merged["_ocr_used"] = pdf.get("_ocr_used", False)
        enriched_nots.append(merged)

    return enriched_nots + other


def _apply_parcel(rec: dict, parcel: Optional[dict], pdf_addr: str = ""):
    if parcel:
        rec.setdefault("prop_address", parcel.get("site_addr", ""))
        rec.setdefault("prop_city",    parcel.get("site_city", ""))
        rec.setdefault("prop_state",   parcel.get("site_state", "AZ"))
        rec.setdefault("prop_zip",     parcel.get("site_zip", ""))
        rec.setdefault("mail_address", parcel.get("mail_addr", ""))
        rec.setdefault("mail_city",    parcel.get("mail_city", ""))
        rec.setdefault("mail_state",   parcel.get("mail_state", "AZ"))
        rec.setdefault("mail_zip",     parcel.get("mail_zip", ""))
    if pdf_addr and not rec.get("prop_address"):
        rec["prop_address"] = pdf_addr
    for k, d in [
        ("prop_address",""), ("prop_city",""), ("prop_state","AZ"), ("prop_zip",""),
        ("mail_address",""), ("mail_city",""), ("mail_state","AZ"), ("mail_zip",""),
    ]:
        rec.setdefault(k, d)


# ===========================================================================
# Seller Scoring
# ===========================================================================

def score_record(rec: dict, all_records: list[dict]) -> tuple[list[str], int]:
    flags: list[str] = []
    score = 30
    cat   = rec.get("cat", "")
    code  = rec.get("doc_type", "")
    owner = (rec.get("owner") or "").upper()
    amt   = rec.get("amount") or 0
    filed = rec.get("filed", "")

    # Category flags
    if cat == "LP":
        flags.append("Lis pendens");              score += 10
    if cat in ("FC", "NOTS"):
        flags.append("Pre-foreclosure");          score += 10
    if cat == "NOTS":
        flags.append("Trustee Sale");             score += 5
    if cat == "TAX":
        flags.append("Tax deed");                 score += 10
    if cat == "JUD":
        flags.append("Judgment lien");            score += 10
    if cat == "LIEN":
        if any(x in code for x in ("IRS", "FED", "CORP")):
            flags.append("Gov/Tax lien");         score += 10
        elif "MECH" in code:
            flags.append("Mechanic lien");        score += 8
        elif "HOA" in code:
            flags.append("HOA lien");             score += 6
        else:
            flags.append("Lien");                 score += 6
    if cat == "PRO":
        flags.append("Probate / estate");         score += 10

    # LP + FC/NOTS combo on same owner
    if cat in ("LP", "FC", "NOTS") and owner:
        owner_cats = {r.get("cat") for r in all_records
                      if r.get("owner", "").upper() == owner
                      and r.get("doc_num") != rec.get("doc_num")}
        if ("LP" in owner_cats and cat in ("FC", "NOTS")) or \
           (cat == "LP" and owner_cats & {"FC", "NOTS"}):
            score += 20

    # Amount
    if amt:
        if   amt > 100_000: score += 15
        elif amt > 50_000:  score += 10

    # New this week
    try:
        if (datetime.now() - datetime.strptime(filed, "%Y-%m-%d")).days <= 7:
            flags.append("New this week"); score += 5
    except Exception:
        pass

    # Address
    if rec.get("prop_address") or rec.get("mail_address"):
        score += 5

    # PDF successfully parsed
    if rec.get("_pdf_ok"):
        score += 3

    # LLC / Corp
    if owner and any(kw in owner for kw in (
        "LLC", "INC", "CORP", " LP", "LTD", "TRUST",
        "HOLDINGS", "PROPERTIES", "INVESTMENTS", "REALTY",
    )):
        flags.append("LLC / corp owner")

    return flags, min(score, 100)


# ===========================================================================
# Excel Export
# ===========================================================================

XLSX_COLS = [
    ("Seller Score",          "score"),
    ("Lead Category",         "cat_label"),
    ("Document Type",         "doc_type"),
    ("Document Number",       "doc_num"),
    ("Date Filed",            "filed"),
    ("Owner / Grantor",       "owner"),
    ("Grantee / Lender",      "grantee"),
    ("Amount / Debt ($)",     "amount"),
    ("Sale Date (NOTS)",      "sale_date"),
    ("Sale Location (NOTS)",  "sale_location"),
    ("Property Address",      "prop_address"),
    ("Property City",         "prop_city"),
    ("Property State",        "prop_state"),
    ("Property Zip",          "prop_zip"),
    ("Mailing Address",       "mail_address"),
    ("Mailing City",          "mail_city"),
    ("Mailing State",         "mail_state"),
    ("Mailing Zip",           "mail_zip"),
    ("Flags",                 "_flags_str"),
    ("Legal Description",     "legal"),
    ("APN (from PDF)",        "_pdf_apn"),
    ("PDF Parsed",            "_pdf_ok_str"),
    ("OCR Used",              "_ocr_str"),
    ("Public Records URL",    "clerk_url"),
]

_COL_W = {
    "Seller Score": 9, "Lead Category": 20, "Document Type": 14,
    "Document Number": 14, "Date Filed": 12, "Owner / Grantor": 32,
    "Grantee / Lender": 28, "Amount / Debt ($)": 16,
    "Sale Date (NOTS)": 14, "Sale Location (NOTS)": 32,
    "Property Address": 32, "Property City": 18, "Property State": 9,
    "Property Zip": 10, "Mailing Address": 32, "Mailing City": 18,
    "Mailing State": 9, "Mailing Zip": 10,
    "Flags": 40, "Legal Description": 40, "APN (from PDF)": 14,
    "PDF Parsed": 10, "OCR Used": 10, "Public Records URL": 12,
}


def write_xlsx(records: list[dict], path: Path):
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        from openpyxl.utils import get_column_letter
    except ImportError:
        log.error("openpyxl not installed — skipping xlsx")
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    wb = Workbook()

    # ── Styles ──────────────────────────────────────────────────────────────
    HDR_FILL = PatternFill("solid", fgColor="1E2433")
    HDR_FONT = Font(name="Arial", bold=True, color="FFFFFF", size=10)
    HDR_ALGN = Alignment(horizontal="center", vertical="center", wrap_text=True)
    THIN     = Border(
        left=Side(style="thin", color="D1D5DB"),
        right=Side(style="thin", color="D1D5DB"),
        top=Side(style="thin", color="D1D5DB"),
        bottom=Side(style="thin", color="D1D5DB"),
    )
    DATA_FONT = Font(name="Arial", size=9)
    URL_FONT  = Font(name="Arial", size=9, color="2563EB", underline="single")
    CTR       = Alignment(horizontal="center", vertical="top")
    LFT       = Alignment(horizontal="left",   vertical="top", wrap_text=True)

    FILL_HIGH = PatternFill("solid", fgColor="FEE2E2")
    FILL_MED  = PatternFill("solid", fgColor="FEF9C3")
    FILL_NONE = PatternFill("solid", fgColor="FFFFFF")

    def row_fill(score): 
        return FILL_HIGH if score >= 70 else FILL_MED if score >= 50 else FILL_NONE

    def prep(r: dict) -> dict:
        out = dict(r)
        out["_flags_str"]  = "; ".join(r.get("flags", []))
        out["_pdf_ok_str"] = "Yes" if r.get("_pdf_ok") else "No"
        out["_ocr_str"]    = "Yes" if r.get("_ocr_used") else "No"
        return out

    def write_header(ws, cols):
        ws.append([c[0] for c in cols])
        for cell in ws[1]:
            cell.font      = HDR_FONT
            cell.fill      = HDR_FILL
            cell.alignment = HDR_ALGN
            cell.border    = THIN
        ws.row_dimensions[1].height = 36

    def write_rows(ws, records_subset, cols):
        for r_idx, rec in enumerate(records_subset, 2):
            p    = prep(rec)
            sc   = p.get("score", 0)
            fill = row_fill(sc)
            ws.append([p.get(fk, "") for _, fk in cols])

            for c_idx, cell in enumerate(ws[r_idx], 1):
                hdr = cols[c_idx - 1][0]
                cell.border = THIN
                cell.fill   = fill
                if hdr == "Seller Score":
                    cell.font = Font(name="Arial", size=10, bold=True,
                                     color=("C0392B" if sc >= 70
                                            else "B45309" if sc >= 50
                                            else "374151"))
                    cell.alignment = CTR
                elif hdr == "Public Records URL" and cell.value:
                    cell.hyperlink = str(cell.value)
                    cell.value     = "View ↗"
                    cell.font      = URL_FONT
                    cell.alignment = CTR
                elif hdr in ("Amount / Debt ($)", "Loan Balance ($)") and cell.value:
                    try:
                        cell.value = float(str(cell.value).replace(",", ""))
                        cell.number_format = '"$"#,##0.00'
                    except (ValueError, TypeError):
                        pass
                    cell.font      = Font(name="Arial", size=9, bold=True)
                    cell.alignment = CTR
                elif hdr in ("PDF Parsed", "OCR Used", "Lead Category",
                             "Document Type", "Date Filed", "Seller Score"):
                    cell.font = DATA_FONT; cell.alignment = CTR
                else:
                    cell.font = DATA_FONT; cell.alignment = LFT
            ws.row_dimensions[r_idx].height = 28

    def set_widths(ws, cols):
        for c_idx, (hdr, _) in enumerate(cols, 1):
            ws.column_dimensions[get_column_letter(c_idx)].width = _COL_W.get(hdr, 16)

    # ── Sheet 1: All Leads ──────────────────────────────────────────────────
    ws1 = wb.active
    ws1.title = "All Leads"
    write_header(ws1, XLSX_COLS)
    write_rows(ws1, records, XLSX_COLS)
    ws1.freeze_panes = "A2"
    ws1.auto_filter.ref = (
        f"A1:{get_column_letter(len(XLSX_COLS))}{ws1.max_row}"
    )
    set_widths(ws1, XLSX_COLS)

    # ── Sheet 2: NOTS Only ──────────────────────────────────────────────────
    NOTS_COLS = [
        ("Seller Score",        "score"),
        ("Owner (Trustor)",     "owner"),
        ("Beneficiary/Lender",  "grantee"),
        ("Loan Balance ($)",    "amount"),
        ("Sale Date",           "sale_date"),
        ("Sale Location",       "sale_location"),
        ("Property Address",    "prop_address"),
        ("Property City",       "prop_city"),
        ("Prop Zip",            "prop_zip"),
        ("Mailing Address",     "mail_address"),
        ("Mailing City",        "mail_city"),
        ("Mailing State",       "mail_state"),
        ("Mailing Zip",         "mail_zip"),
        ("Date Filed",          "filed"),
        ("Document Number",     "doc_num"),
        ("APN",                 "_pdf_apn"),
        ("Flags",               "_flags_str"),
        ("PDF Parsed",          "_pdf_ok_str"),
        ("OCR Used",            "_ocr_str"),
        ("Public Records URL",  "clerk_url"),
    ]

    ws2 = wb.create_sheet("NOTS — Trustee Sales")
    HDR_FILL_2 = PatternFill("solid", fgColor="1E3A5F")
    ws2.append([c[0] for c in NOTS_COLS])
    for cell in ws2[1]:
        cell.font = HDR_FONT; cell.fill = HDR_FILL_2
        cell.alignment = HDR_ALGN; cell.border = THIN
    ws2.row_dimensions[1].height = 36
    write_rows(ws2, [r for r in records if r.get("cat") == "NOTS"], NOTS_COLS)
    ws2.freeze_panes = "A2"
    ws2.auto_filter.ref = (
        f"A1:{get_column_letter(len(NOTS_COLS))}{ws2.max_row}"
    )
    for c_idx, (hdr, _) in enumerate(NOTS_COLS, 1):
        ws2.column_dimensions[get_column_letter(c_idx)].width = _COL_W.get(hdr, 18)

    # ── Sheet 3: Summary ────────────────────────────────────────────────────
    ws3 = wb.create_sheet("Summary")
    ws3["A1"] = "Maricopa County Motivated Seller Leads"
    ws3["A1"].font = Font(name="Arial", size=14, bold=True)
    ws3["A2"] = f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    ws3["A2"].font = Font(name="Arial", size=10, color="6B7280")

    nots_subset = [r for r in records if r.get("cat") == "NOTS"]
    summary = [
        ("", ""),
        ("Total Leads",                len(records)),
        ("NOTS (Trustee Sale)",        len(nots_subset)),
        ("NOTS PDFs parsed",           sum(1 for r in records if r.get("_pdf_ok"))),
        ("NOTS OCR used",              sum(1 for r in records if r.get("_ocr_used"))),
        ("With property address",      sum(1 for r in records if r.get("prop_address"))),
        ("With mailing address",       sum(1 for r in records if r.get("mail_address"))),
        ("Score 70+ (Hot)",            sum(1 for r in records if r.get("score", 0) >= 70)),
        ("Score 50–69 (Warm)",         sum(1 for r in records if 50 <= r.get("score", 0) < 70)),
        ("Score < 50",                 sum(1 for r in records if r.get("score", 0) < 50)),
        ("", ""),
        ("By Category", "Count"),
    ]
    cats: dict[str, int] = {}
    for r in records:
        lbl = r.get("cat_label", "Other")
        cats[lbl] = cats.get(lbl, 0) + 1
    for lbl, cnt in sorted(cats.items(), key=lambda x: -x[1]):
        summary.append((lbl, cnt))

    for i, (lbl, val) in enumerate(summary, 3):
        ws3.cell(i, 1, lbl).font = Font(name="Arial", size=10, bold=bool(lbl))
        ws3.cell(i, 2, val).font = Font(name="Arial", size=10)
    ws3.column_dimensions["A"].width = 32
    ws3.column_dimensions["B"].width = 14

    wb.save(path)
    log.info(
        f"Excel → {path}  "
        f"({len(records)} rows, {len(nots_subset)} NOTS)"
    )


# ===========================================================================
# GHL CSV
# ===========================================================================

GHL_COLUMNS = [
    "First Name", "Last Name", "Mailing Address", "Mailing City",
    "Mailing State", "Mailing Zip", "Property Address", "Property City",
    "Property State", "Property Zip", "Lead Type", "Document Type",
    "Date Filed", "Document Number", "Amount/Debt Owed",
    "Seller Score", "Motivated Seller Flags", "Source", "Public Records URL",
]


def _split_name(full: str) -> tuple[str, str]:
    if not full:
        return "", ""
    if "," in full:
        parts = full.split(",", 1)
        return parts[1].strip().title(), parts[0].strip().title()
    toks = full.strip().split()
    return (toks[0].title(), " ".join(toks[1:]).title()) if len(toks) > 1 else ("", toks[0].title())


def write_ghl_csv(records: list[dict], path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=GHL_COLUMNS)
        w.writeheader()
        for r in records:
            fn, ln = _split_name(r.get("owner") or "")
            amt = r.get("amount")
            w.writerow({
                "First Name":           fn,
                "Last Name":            ln,
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
                "Amount/Debt Owed":     f"{amt:,.2f}" if amt else "",
                "Seller Score":         r.get("score", ""),
                "Motivated Seller Flags": "; ".join(r.get("flags", [])),
                "Source":               "Maricopa County Recorder",
                "Public Records URL":   r.get("clerk_url", ""),
            })
    log.info(f"GHL CSV → {path} ({len(records)} rows)")


# ===========================================================================
# Main
# ===========================================================================

async def main():
    end_dt    = datetime.now()
    start_dt  = end_dt - timedelta(days=LOOKBACK_DAYS)
    start_str = start_dt.strftime("%m/%d/%Y")
    end_str   = end_dt.strftime("%m/%d/%Y")

    log.info("=" * 60)
    log.info("Maricopa Motivated Seller Scraper v2.0")
    log.info(f"Date range: {start_str} → {end_str}")
    log.info("=" * 60)

    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0 (compatible; scraper/2.0)"

    # 1. Parcel data
    parcels = ParcelIndex()
    parcel_ok = parcels.load()

    # 2. Scrape portal
    all_raw: list[dict] = []
    for code, meta in LEAD_TYPES.items():
        try:
            recs = await scrape_clerk(code, start_str, end_str)
            for r in recs:
                r["cat"]       = meta["cat"]
                r["cat_label"] = meta["label"]
                r["owner"]     = r.pop("grantor", "") or ""
            all_raw.extend(recs)
            log.info(f"  ✓ {code}: {len(recs)}")
        except Exception as exc:
            log.error(f"  ✗ {code}: {exc}")

    log.info(f"Raw records: {len(all_raw)}")

    # 3. Dedup
    seen: set[str] = set()
    deduped: list[dict] = []
    for r in all_raw:
        key = r.get("doc_num", "")
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        deduped.append(r)
    log.info(f"After dedup: {len(deduped)}")

    # 4. NOTS PDF pipeline
    enriched = await run_nots_pipeline(deduped, parcels, session)

    # 5. Parcel enrichment for non-NOTS
    for rec in enriched:
        if rec.get("cat") != "NOTS":
            parcel = parcels.lookup(rec.get("owner", ""))
            _apply_parcel(rec, parcel)

    # 6. Score
    for rec in enriched:
        try:
            flags, score = score_record(rec, enriched)
            rec["flags"] = flags
            rec["score"] = score
        except Exception:
            rec.setdefault("flags", []); rec.setdefault("score", 30)

    enriched.sort(key=lambda r: r.get("score", 0), reverse=True)

    # 7. Save JSON
    with_address = sum(1 for r in enriched if r.get("prop_address") or r.get("mail_address"))
    payload = {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Maricopa County Recorder",
        "date_range":   {"start": start_str, "end": end_str},
        "total":        len(enriched),
        "with_address": with_address,
        "records":      enriched,
    }
    for path in OUTPUT_PATHS:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, indent=2, default=str))
            log.info(f"JSON → {path}")
        except Exception as exc:
            log.error(f"JSON save failed ({path}): {exc}")

    # 8. Excel
    try:
        write_xlsx(enriched, XLSX_PATH)
    except Exception as exc:
        log.error(f"Excel failed: {exc}\n{traceback.format_exc()}")

    # 9. GHL CSV
    try:
        write_ghl_csv(enriched, GHL_CSV_PATH)
    except Exception as exc:
        log.error(f"GHL CSV failed: {exc}")

    nots_c = sum(1 for r in enriched if r.get("cat") == "NOTS")
    pdf_ok = sum(1 for r in enriched if r.get("_pdf_ok"))
    log.info("=" * 60)
    log.info(f"Total leads : {len(enriched)}")
    log.info(f"NOTS        : {nots_c}  (PDFs parsed: {pdf_ok})")
    log.info(f"With address: {with_address}")
    log.info(f"Score 70+   : {sum(1 for r in enriched if r.get('score',0) >= 70)}")
    log.info(f"Parcel data : {'✓' if parcel_ok else '✗ unavailable'}")
    log.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
