"""
Build IPL player PDFs + metadata + images.

Pipeline:
1) Try loading player names from ESPN Cricinfo auction page.
2) For each player, fetch public profile fields from Wikipedia/Wikidata.
3) Download player image (if available) into data/images/.
4) Generate one PDF per player in data/.
5) Save UI metadata in data/player_metadata.json.

Usage:
  python build_ipl_auction_dataset.py \
    --auction-url "https://www.espncricinfo.com/auction/ipl-2026-auction-1515016/all-players" \
    --output-dir data
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import time
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.pdfgen import canvas


WIKI_API = "https://en.wikipedia.org/w/api.php"
WD_API = "https://www.wikidata.org/w/api.php"
UA = {
    "User-Agent": "CricketRAG-IPLBuilder/1.0 (local project; public data usage)",
    "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
}
SESSION = requests.Session()
SESSION.headers.update(UA)
REQUEST_INTERVAL_SECONDS = 1.2
_LAST_REQUEST_TS = 0.0


def get(url: str, params: Optional[Dict] = None, timeout: int = 30) -> requests.Response:
    global _LAST_REQUEST_TS
    retries = 4

    for attempt in range(retries):
        now = time.monotonic()
        wait = REQUEST_INTERVAL_SECONDS - (now - _LAST_REQUEST_TS)
        if wait > 0:
            time.sleep(wait)

        r = SESSION.get(url, params=params, timeout=timeout)
        _LAST_REQUEST_TS = time.monotonic()

        if r.status_code in (403, 429, 500, 502, 503, 504):
            if attempt < retries - 1:
                time.sleep((attempt + 1) * REQUEST_INTERVAL_SECONDS)
                continue
        r.raise_for_status()
        return r

    # Should not be reached because raise_for_status above will throw.
    raise RuntimeError(f"Request failed for URL: {url}")


def extract_players_from_espn_html(html: str) -> List[Dict[str, str]]:
    """
    Best-effort parse of ESPN auction HTML.
    Returns list[{"name":..., "ipl_team":...}] where team may be empty.
    """
    players: List[Dict[str, str]] = []

    # JSON-in-script fallback: look for name/team fragments.
    # This is intentionally broad because ESPN page structures can change.
    name_candidates = set(re.findall(r'"name"\s*:\s*"([^"]+)"', html))
    team_candidates = re.findall(r'"team(?:Name)?"\s*:\s*"([^"]+)"', html)

    if name_candidates:
        for n in sorted(name_candidates):
            # Avoid noise values.
            if len(n.split()) >= 2 and not n.lower().startswith(("ipl", "auction")):
                players.append({"name": n.strip(), "ipl_team": ""})

    # HTML table fallback
    if not players:
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("tr")
        for row in rows:
            tds = [td.get_text(" ", strip=True) for td in row.select("td")]
            if not tds:
                continue
            # Heuristic: one cell with a person-like name.
            maybe_name = ""
            for cell in tds:
                if re.match(r"^[A-Z][A-Za-z.' -]{2,}$", cell) and len(cell.split()) <= 4:
                    maybe_name = cell
                    break
            if maybe_name:
                players.append({"name": maybe_name, "ipl_team": ""})

    # Deduplicate
    seen = set()
    out = []
    for p in players:
        k = p["name"].strip().lower()
        if k and k not in seen:
            seen.add(k)
            out.append(p)
    return out


def extract_players_from_espn(auction_url: str) -> List[Dict[str, str]]:
    r = get(auction_url, timeout=40)
    return extract_players_from_espn_html(r.text)


def parse_cricinfo_player_url(player_url: str) -> Tuple[str, str]:
    """
    Parse Cricinfo player URL like:
    https://www.espncricinfo.com/cricketers/arshdeep-singh-1125976
    Returns (name, player_id)
    """
    m = re.search(r"/cricketers/([a-z0-9-]+)-(\d+)", player_url)
    if not m:
        raise ValueError(f"Invalid Cricinfo player URL: {player_url}")
    slug, pid = m.group(1), m.group(2)
    name = slug.replace("-", " ").title()
    return name, pid


def fetch_espn_statsguru_summary(player_id: str, stat_type: str) -> Dict[str, Dict[str, str]]:
    """
    Fetch Statsguru summary table for batting/bowling/allround.
    Returns dict keyed by format (ODIs, T20Is, etc.) with column:value.
    """
    url = f"https://stats.espncricinfo.com/ci/engine/player/{player_id}.html"
    params = {"class": "11", "template": "results", "type": stat_type}
    html = get(url, params=params, timeout=40).text
    soup = BeautifulSoup(html, "html.parser")

    tables = soup.select("table.engineTable")
    for table in tables:
        rows = table.select("tr")
        if len(rows) < 2:
            continue
        headers = [clean(th.get_text(" ", strip=True)) for th in rows[0].select("th")]
        if "Mat" not in headers:
            continue
        out: Dict[str, Dict[str, str]] = {}
        for row in rows[1:]:
            tds = [clean(td.get_text(" ", strip=True)) for td in row.select("td")]
            if len(tds) != len(headers):
                continue
            fmt = tds[0]
            if not fmt or fmt.lower() in {"span", "overall"}:
                continue
            out[fmt] = {headers[i]: tds[i] for i in range(1, len(headers))}
        if out:
            return out
    return {}


def fetch_espn_profile_text(player_id: str) -> Dict[str, str]:
    """
    Fetch basic profile label line from Statsguru page text.
    Example: 'Arshdeep Singh - left-hand bat; left-arm medium-fast - Player profile'
    """
    url = f"https://stats.espncricinfo.com/ci/engine/player/{player_id}.html"
    params = {"class": "11", "type": "allround"}
    text = get(url, params=params, timeout=40).text
    line_m = re.search(r"([A-Za-z .'-]+)\s*-\s*([^<]+?)\s*-\s*Player profile", text)
    born_m = re.search(r"Born\\s+([A-Za-z0-9, ]+)", text)
    profile = {}
    if line_m:
        profile["profile_line"] = clean(line_m.group(0))
    if born_m:
        profile["born"] = clean(born_m.group(1))
    return profile


def fetch_wiki_player(name: str) -> Dict:
    params = {
        "action": "query",
        "format": "json",
        "prop": "extracts|pageprops|info",
        "inprop": "url",
        "explaintext": 1,
        "redirects": 1,
        "titles": name,
        "origin": "*",
    }
    payload = get(WIKI_API, params=params).json()
    pages = payload.get("query", {}).get("pages", {})
    page = next(iter(pages.values()))
    if "missing" in page:
        raise ValueError(f"Wikipedia page not found: {name}")

    title = page.get("title", name)
    extract = sanitize_for_pdf(page.get("extract", ""))
    url = page.get("fullurl", f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}")
    qid = page.get("pageprops", {}).get("wikibase_item")

    html = get(
        WIKI_API,
        params={
            "action": "parse",
            "format": "json",
            "redirects": 1,
            "page": title,
            "prop": "text",
            "origin": "*",
        },
    ).json()
    html_text = html.get("parse", {}).get("text", {}).get("*", "")
    info = parse_infobox(html_text)
    image_url = fetch_page_image(title)

    return {
        "name": title,
        "extract": build_safe_summary(extract),
        "url": url,
        "qid": qid,
        "infobox": info,
        "image_url": image_url,
    }


def parse_infobox(html: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    box = soup.find("table", class_=lambda c: c and "infobox" in c)
    result: Dict[str, str] = {}
    if not box:
        return result
    for tr in box.find_all("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not th or not td:
            continue
        key = clean(th.get_text(" ", strip=True))
        val = clean(td.get_text(" ", strip=True))
        if key and val:
            result[key] = val
    return result


def fetch_page_image(title: str) -> str:
    params = {
        "action": "query",
        "format": "json",
        "prop": "pageimages",
        "pithumbsize": 600,
        "redirects": 1,
        "titles": title,
        "origin": "*",
    }
    payload = get(WIKI_API, params=params).json()
    pages = payload.get("query", {}).get("pages", {})
    page = next(iter(pages.values()))
    return page.get("thumbnail", {}).get("source", "")


def derive_basic_fields(infobox: Dict[str, str], ipl_team: str) -> Dict[str, str]:
    born = infobox.get("Born", "")
    age = extract_age_from_born(born)
    runs = find_value(infobox, ["Runs", "Runs scored", "IPL runs"])
    wickets = find_value(infobox, ["Wickets", "IPL wickets"])
    matches = find_value(infobox, ["Matches", "No. of IPL matches", "IPL matches"])
    role = infobox.get("Role", "")
    country = infobox.get("National side", "")
    if not country:
        country = find_value(infobox, ["Country"])
    current_team = ipl_team or find_value(infobox, ["Current team", "Team"])

    return {
        "age": age,
        "runs": runs,
        "wickets": wickets,
        "matches": matches,
        "role": role,
        "country": country,
        "ipl_team": current_team,
    }


def extract_age_from_born(born_text: str) -> str:
    m = re.search(r"(\d{4})", born_text)
    if not m:
        return ""
    year = int(m.group(1))
    now = dt.datetime.utcnow().year
    if year < 1900 or year > now:
        return ""
    return str(now - year)


def find_value(infobox: Dict[str, str], keys: List[str]) -> str:
    for k in keys:
        if k in infobox and infobox[k]:
            return infobox[k]
    # relaxed contains
    for actual_key, v in infobox.items():
        ak = actual_key.lower()
        if any(k.lower() in ak for k in keys) and v:
            return v
    return ""


def clean(v: str) -> str:
    v = re.sub(r"\[[0-9]+\]", "", v)
    v = re.sub(r"\s+", " ", v).strip()
    return sanitize_for_pdf(v)


def sanitize_for_pdf(text: str) -> str:
    """Normalize text to avoid unsupported glyphs in default PDF fonts."""
    if not text:
        return ""
    t = unicodedata.normalize("NFKD", text)
    # Remove control chars and non-printables.
    t = "".join(ch for ch in t if ch == "\n" or (ord(ch) >= 32 and ch.isprintable()))
    # Default reportlab fonts handle ASCII reliably.
    t = t.encode("ascii", "ignore").decode("ascii")
    t = re.sub(r"\s+", " ", t).strip()
    return t


def build_safe_summary(extract: str) -> str:
    """
    Keep summary conservative to reduce noisy/dated claims.
    Uses only first 1-2 sentences and removes bracketed native-script segments.
    """
    if not extract:
        return "N/A"
    txt = re.sub(r"\([^)]{0,120}\)", "", extract)
    parts = re.split(r"(?<=[.!?])\s+", txt)
    summary = " ".join(parts[:2]).strip()
    return sanitize_for_pdf(summary) or "N/A"


def download_image(url: str, out_path: Path) -> str:
    if not url:
        return ""
    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        r = get(url, timeout=40)
        out_path.write_bytes(r.content)
        return str(out_path)
    except Exception:
        return ""


def draw_wrapped(c: canvas.Canvas, text: str, x: float, y: float, width: float) -> float:
    words = text.split()
    if not words:
        return y
    line = words[0]
    for w in words[1:]:
        cand = f"{line} {w}"
        if c.stringWidth(cand, "Helvetica", 10) <= width:
            line = cand
        else:
            c.drawString(x, y, line)
            y -= 14
            line = w
    c.drawString(x, y, line)
    return y - 14


def write_pdf(player: Dict, output_pdf: Path, image_path: str) -> None:
    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    c = canvas.Canvas(str(output_pdf), pagesize=A4)
    w, h = A4
    x = 2 * cm
    y = h - 2 * cm

    c.setFont("Helvetica-Bold", 14)
    c.drawString(x, y, sanitize_for_pdf(f"IPL Player Profile: {player['name']}"))
    y -= 20

    if image_path and Path(image_path).exists():
        c.drawImage(image_path, x, y - 6 * cm, width=4.5 * cm, height=6 * cm, preserveAspectRatio=True)
        text_x = x + 5 * cm
    else:
        text_x = x

    b = player["basic"]
    c.setFont("Helvetica", 10)
    basics = [
        sanitize_for_pdf(f"Age: {b.get('age', '') or 'N/A'}"),
        sanitize_for_pdf(f"Country: {b.get('country', '') or 'N/A'}"),
        sanitize_for_pdf(f"Role: {b.get('role', '') or 'N/A'}"),
        sanitize_for_pdf(f"IPL Current Team: {b.get('ipl_team', '') or 'N/A'}"),
        sanitize_for_pdf(f"Matches: {b.get('matches', '') or 'N/A'}"),
        sanitize_for_pdf(f"Runs: {b.get('runs', '') or 'N/A'}"),
        sanitize_for_pdf(f"Wickets: {b.get('wickets', '') or 'N/A'}"),
    ]
    yy = y
    for line in basics:
        c.drawString(text_x, yy, line)
        yy -= 14

    y = y - 7 * cm
    c.setFont("Helvetica-Bold", 11)
    c.drawString(x, y, "Career Summary")
    y -= 16
    c.setFont("Helvetica", 10)
    y = draw_wrapped(c, sanitize_for_pdf(player.get("extract", "N/A")), x, y, w - 4 * cm)

    y -= 8
    espn_stats = player.get("espn_stats", {})
    if espn_stats:
        c.setFont("Helvetica-Bold", 11)
        c.drawString(x, y, "ESPN Cricinfo Stats Summary")
        y -= 16
        c.setFont("Helvetica", 10)
        for label, value in espn_stats.items():
            if not value:
                continue
            y = draw_wrapped(c, sanitize_for_pdf(f"{label}: {value}"), x, y, w - 4 * cm)
            if y < 3 * cm:
                c.showPage()
                y = h - 2 * cm
                c.setFont("Helvetica", 10)
        y -= 8

    c.setFont("Helvetica-Bold", 11)
    c.drawString(x, y, "Source")
    y -= 14
    c.setFont("Helvetica", 9)
    source_line = sanitize_for_pdf(
        f"Wikipedia: {player.get('wiki_url', '')} | Retrieved: {dt.datetime.utcnow().isoformat()} UTC"
    )
    draw_wrapped(c, source_line, x, y, w - 4 * cm)
    c.save()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--auction-url", default="")
    parser.add_argument(
        "--player-url",
        default="",
        help="Single ESPN Cricinfo player URL (e.g. Arshdeep Singh URL).",
    )
    parser.add_argument(
        "--auction-html-file",
        default="",
        help="Local HTML file saved from the ESPN auction page (fallback for 403).",
    )
    parser.add_argument(
        "--players-file",
        default="",
        help="Optional text file with one player name per line.",
    )
    parser.add_argument(
        "--players",
        nargs="*",
        default=None,
        help='Optional direct list, e.g. --players "Virat Kohli" "MS Dhoni"',
    )
    parser.add_argument("--output-dir", default="data")
    parser.add_argument("--limit", type=int, default=0, help="Optional cap for testing.")
    parser.add_argument(
        "--request-interval",
        type=float,
        default=1.2,
        help="Seconds between outbound HTTP requests (to reduce forbidden/rate limits).",
    )
    args = parser.parse_args()
    global REQUEST_INTERVAL_SECONDS
    REQUEST_INTERVAL_SECONDS = max(0.2, args.request_interval)

    out_dir = Path(args.output_dir)
    images_dir = out_dir / "images"
    metadata_path = out_dir / "player_metadata.json"

    players: List[Dict[str, str]] = []
    if args.player_url:
        name, pid = parse_cricinfo_player_url(args.player_url)
        players = [{"name": name, "ipl_team": "", "espn_player_id": pid, "player_url": args.player_url}]
    elif args.players:
        players = [{"name": p, "ipl_team": ""} for p in args.players]
    elif args.players_file:
        players_file_path = Path(args.players_file)
        if not players_file_path.exists():
            raise FileNotFoundError(
                f"Players file not found: {players_file_path}. "
                "Create it with one player name per line, or use --players / --auction-url."
            )
        lines = players_file_path.read_text(encoding="utf-8").splitlines()
        players = [{"name": ln.strip(), "ipl_team": ""} for ln in lines if ln.strip()]
    elif args.auction_html_file:
        html_path = Path(args.auction_html_file)
        if not html_path.exists():
            raise FileNotFoundError(f"Auction HTML file not found: {html_path}")
        players = extract_players_from_espn_html(html_path.read_text(encoding="utf-8"))
    elif args.auction_url:
        try:
            players = extract_players_from_espn(args.auction_url)
        except requests.HTTPError as exc:
            if getattr(exc.response, "status_code", None) == 403:
                raise RuntimeError(
                    "ESPN returned 403 (bot protection). Save the auction page HTML "
                    "from your browser and rerun with --auction-html-file."
                ) from exc
            raise
    else:
        raise RuntimeError(
            "Provide one of --auction-url, --auction-html-file, --players-file, or --players."
        )
    if args.limit > 0:
        players = players[: args.limit]
    if not players:
        raise RuntimeError("No players parsed from the auction page.")

    existing_metadata: Dict[str, Dict] = {}
    if metadata_path.exists():
        try:
            existing_metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except Exception:
            existing_metadata = {}

    metadata: Dict[str, Dict] = {}
    success_count = 0
    for i, p in enumerate(players, start=1):
        raw_name = p["name"]
        try:
            wp = fetch_wiki_player(raw_name)
            basic = derive_basic_fields(wp["infobox"], p.get("ipl_team", ""))
            espn_player_id = p.get("espn_player_id", "")
            espn_stats_summary: Dict[str, str] = {}
            if espn_player_id:
                try:
                    profile = fetch_espn_profile_text(espn_player_id)
                    bat = fetch_espn_statsguru_summary(espn_player_id, "batting")
                    bowl = fetch_espn_statsguru_summary(espn_player_id, "bowling")
                    t20_bat = bat.get("T20Is", {})
                    t20_bowl = bowl.get("T20Is", {})
                    odi_bat = bat.get("ODIs", {})
                    odi_bowl = bowl.get("ODIs", {})

                    espn_stats_summary = {
                        "Profile": profile.get("profile_line", ""),
                        "Born": profile.get("born", ""),
                        "T20I matches": t20_bat.get("Mat", ""),
                        "T20I runs": t20_bat.get("Runs", ""),
                        "T20I wickets": t20_bowl.get("Wkts", ""),
                        "ODI matches": odi_bat.get("Mat", ""),
                        "ODI runs": odi_bat.get("Runs", ""),
                        "ODI wickets": odi_bowl.get("Wkts", ""),
                    }
                    # Prefer Cricinfo figures if present.
                    basic["matches"] = basic["matches"] or t20_bat.get("Mat", "")
                    basic["runs"] = basic["runs"] or t20_bat.get("Runs", "")
                    basic["wickets"] = basic["wickets"] or t20_bowl.get("Wkts", "")
                except Exception as exc:
                    print(f"[{i}/{len(players)}] WARN ESPN stats unavailable for {raw_name}: {exc}")

            file_stem = re.sub(r"[^A-Za-z0-9_-]+", "_", wp["name"]).strip("_")
            image_file = images_dir / f"{file_stem}.jpg"
            image_local = download_image(wp.get("image_url", ""), image_file)
            pdf_path = out_dir / f"{file_stem}.pdf"

            record = {
                "name": wp["name"],
                "basic": basic,
                "extract": wp.get("extract", ""),
                "wiki_url": wp.get("url", ""),
                "image_path": image_local,
                "espn_stats": espn_stats_summary,
            }
            write_pdf(record, pdf_path, image_local)

            metadata[wp["name"]] = {
                "name": wp["name"],
                "pdf_path": str(pdf_path),
                "image_path": image_local,
                "espn_player_id": espn_player_id,
                "espn_player_url": p.get("player_url", ""),
                "espn_stats": espn_stats_summary,
                **basic,
            }
            success_count += 1
            print(f"[{i}/{len(players)}] OK {wp['name']}")
        except Exception as exc:
            print(f"[{i}/{len(players)}] ERROR {raw_name}: {exc}")

    if success_count == 0 and existing_metadata:
        print(
            "No new players were generated successfully. "
            "Keeping existing player_metadata.json unchanged."
        )
    else:
        merged = {**existing_metadata, **metadata}
        metadata_path.write_text(json.dumps(merged, indent=2), encoding="utf-8")
        print(f"Saved metadata: {metadata_path}")


if __name__ == "__main__":
    main()
