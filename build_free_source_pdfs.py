"""
Build cricketer PDF documents from free/public Wikimedia sources only.

Sources used:
- Wikipedia API (free public endpoint)
- Wikidata API (free public endpoint)

Each generated PDF includes source URLs and retrieval timestamp.

Usage:
  python build_free_source_pdfs.py --players "Virat Kohli" "MS Dhoni" "Joe Root"
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import time
from pathlib import Path
from typing import Dict, List, Tuple

import requests
from bs4 import BeautifulSoup
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.pdfgen import canvas


WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
DEFAULT_HEADERS = {
    # Wikimedia requires a descriptive UA; generic clients are often blocked.
    "User-Agent": "CricketRAGDatasetBuilder/1.0 (offline-rag-project; contact: local-user)",
    "Accept": "application/json",
}


def get_json(url: str, params: Dict, timeout: int = 30) -> Dict:
    """HTTP GET with retries and Wikimedia-friendly headers."""
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    # `origin=*` is harmless server-side and can reduce some gateway/CORS issues.
    if "origin" not in params:
        params["origin"] = "*"

    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            r = session.get(url, params=params, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(1.5 * (attempt + 1))
                continue
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            last_exc = exc
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"Request failed after retries for {url}: {last_exc}")


def fetch_wikipedia_page_data(player_name: str) -> Dict:
    """Fetch plaintext extract, canonical URL, wikidata id, and infobox fields."""
    params = {
        "action": "query",
        "format": "json",
        "prop": "extracts|info|pageprops",
        "inprop": "url",
        "explaintext": 1,
        "titles": player_name,
        "redirects": 1,
    }
    payload = get_json(WIKIPEDIA_API, params=params, timeout=30)
    pages = payload.get("query", {}).get("pages", {})
    page = next(iter(pages.values()))

    if "missing" in page:
        raise ValueError(f"Wikipedia page not found for: {player_name}")

    title = page.get("title", player_name)
    extract = page.get("extract", "").strip()
    fullurl = page.get("fullurl", f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}")
    wikibase_item = page.get("pageprops", {}).get("wikibase_item")

    html = fetch_wikipedia_html(title)
    infobox = parse_infobox_fields(html)
    return {
        "title": title,
        "extract": extract,
        "url": fullurl,
        "wikibase_item": wikibase_item,
        "infobox": infobox,
    }


def fetch_wikipedia_html(page_title: str) -> str:
    """Fetch rendered HTML for infobox parsing."""
    params = {
        "action": "parse",
        "format": "json",
        "page": page_title,
        "prop": "text",
        "redirects": 1,
    }
    payload = get_json(WIKIPEDIA_API, params=params, timeout=30)
    return payload.get("parse", {}).get("text", {}).get("*", "")


def parse_infobox_fields(html: str) -> Dict[str, str]:
    """Extract key/value rows from the page infobox."""
    soup = BeautifulSoup(html, "html.parser")
    box = soup.find("table", class_=lambda c: c and "infobox" in c)
    fields: Dict[str, str] = {}
    if not box:
        return fields

    for row in box.find_all("tr"):
        th = row.find("th")
        td = row.find("td")
        if not th or not td:
            continue
        key = clean_text(th.get_text(" ", strip=True))
        val = clean_text(td.get_text(" ", strip=True))
        if key and val:
            fields[key] = val
    return fields


def fetch_wikidata_labels(entity_id: str) -> Dict[str, str]:
    """Get a few human-friendly labels from Wikidata entity claims."""
    params = {
        "action": "wbgetentities",
        "format": "json",
        "ids": entity_id,
        "languages": "en",
        "props": "labels|claims",
    }
    payload = get_json(WIKIDATA_API, params=params, timeout=30)
    entity = payload.get("entities", {}).get(entity_id, {})
    claims = entity.get("claims", {})

    out: Dict[str, str] = {}
    dob = extract_time_claim(claims, "P569")  # date of birth
    if dob:
        out["Date of birth (Wikidata)"] = dob
    return out


def extract_time_claim(claims: Dict, prop_id: str) -> str:
    vals = claims.get(prop_id, [])
    if not vals:
        return ""
    try:
        raw = vals[0]["mainsnak"]["datavalue"]["value"]["time"]
        m = re.match(r"^\+(\d{4}-\d{2}-\d{2})T", raw)
        return m.group(1) if m else ""
    except Exception:
        return ""


def clean_text(value: str) -> str:
    value = re.sub(r"\[[0-9]+\]", "", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def pick_relevant_infobox_fields(infobox: Dict[str, str]) -> List[Tuple[str, str]]:
    """Keep only fields useful for cricketer QA."""
    wanted_prefixes = [
        "Born",
        "Role",
        "Batting",
        "Bowling",
        "National side",
        "Test",
        "ODI",
        "T20I",
        "Career statistics",
    ]
    out: List[Tuple[str, str]] = []
    for k, v in infobox.items():
        if any(k.startswith(prefix) for prefix in wanted_prefixes):
            out.append((k, v))
    return out


def write_pdf(output_path: Path, title: str, sections: List[Tuple[str, str]]) -> None:
    """Write simple readable PDF using reportlab."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    c = canvas.Canvas(str(output_path), pagesize=A4)
    width, height = A4
    x = 2 * cm
    y = height - 2 * cm
    max_width = width - 4 * cm
    line_h = 14

    c.setFont("Helvetica-Bold", 14)
    c.drawString(x, y, title)
    y -= 22

    c.setFont("Helvetica", 10)
    for heading, body in sections:
        if y < 3 * cm:
            c.showPage()
            c.setFont("Helvetica", 10)
            y = height - 2 * cm

        c.setFont("Helvetica-Bold", 11)
        c.drawString(x, y, heading)
        y -= 14

        c.setFont("Helvetica", 10)
        for line in wrap_text(body, c, max_width):
            if y < 2.5 * cm:
                c.showPage()
                c.setFont("Helvetica", 10)
                y = height - 2 * cm
            c.drawString(x, y, line)
            y -= line_h
        y -= 8

    c.save()


def wrap_text(text: str, c: canvas.Canvas, max_width: float) -> List[str]:
    words = text.split()
    if not words:
        return [""]
    lines = []
    current = words[0]
    for w in words[1:]:
        candidate = f"{current} {w}"
        if c.stringWidth(candidate, "Helvetica", 10) <= max_width:
            current = candidate
        else:
            lines.append(current)
            current = w
    lines.append(current)
    return lines


def build_player_pdf(player_name: str, output_dir: Path) -> Path:
    data = fetch_wikipedia_page_data(player_name)
    title = data["title"]
    infobox = data["infobox"]
    wikidata_id = data["wikibase_item"]

    sections: List[Tuple[str, str]] = []
    sections.append(("Player Name", title))

    relevant = pick_relevant_infobox_fields(infobox)
    if relevant:
        sections.append(
            (
                "Infobox Details (Wikipedia)",
                " | ".join([f"{k}: {v}" for k, v in relevant]),
            )
        )

    summary = data["extract"] or "No summary text available."
    sections.append(("Biography and Career Summary (Wikipedia)", summary))

    if wikidata_id:
        wdata = fetch_wikidata_labels(wikidata_id)
        if wdata:
            sections.append(
                ("Wikidata Fields", " | ".join([f"{k}: {v}" for k, v in wdata.items()]))
            )

    retrieved_at = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    sections.append(
        (
            "Data Source and License",
            (
                "Primary source: Wikipedia (CC BY-SA), free public access. "
                f"URL: {data['url']}. "
                f"Wikidata entity: {wikidata_id or 'N/A'}. "
                f"Retrieved: {retrieved_at}."
            ),
        )
    )

    safe_name = re.sub(r"[^A-Za-z0-9_-]+", "_", title).strip("_")
    output_path = output_dir / f"{safe_name}.pdf"
    write_pdf(output_path, f"Cricketer Profile: {title}", sections)
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate free-source cricketer PDFs.")
    parser.add_argument(
        "--players",
        nargs="+",
        required=True,
        help='One or more player names, e.g. --players "Virat Kohli" "MS Dhoni"',
    )
    parser.add_argument("--output-dir", default="data", help="Directory for generated PDFs.")
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    for player in args.players:
        try:
            pdf_path = build_player_pdf(player, out_dir)
            print(f"[OK] Generated: {pdf_path}")
        except Exception as exc:
            print(f"[ERROR] {player}: {exc}")


if __name__ == "__main__":
    main()
