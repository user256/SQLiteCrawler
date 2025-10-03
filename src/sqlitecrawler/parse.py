from __future__ import annotations
from urllib.parse import urljoin, urlsplit, urlunsplit
from typing import Iterable, Tuple
from bs4 import BeautifulSoup
from defusedxml import ElementTree as SafeET

# ------------------ URL helpers ------------------

def normalize_url(base: str, href: str) -> str:
    u = urljoin(base, href)
    parts = list(urlsplit(u))
    # keep query; drop fragment
    parts[4] = ""
    return urlunsplit(parts)

# ------------------ classification ------------------

ASSET_EXT = {
    "image": {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".avif"},
    "asset": {".css", ".js", ".pdf", ".zip", ".woff", ".woff2", ".ttf"},
}

def classify(content_type: str | None, url: str) -> str:
    ct = (content_type or "").lower()
    if "xml" in ct and "sitemap" in ct:
        return "sitemap"  # concrete file; may still be an index
    if ct.startswith("text/html"):
        return "html"
    # fallback on extension
    for kind, exts in ASSET_EXT.items():
        for ext in exts:
            if url.lower().endswith(ext):
                return "image" if kind == "image" else "asset"
    return "other"

# Heuristic to detect sitemap index vs urlset

def sniff_sitemap_kind(xml_text: str) -> str:
    try:
        root = SafeET.fromstring(xml_text.encode("utf-8"))
        tag = root.tag.lower()
        if tag.endswith("sitemapindex"):
            return "sitemap_index"
        if tag.endswith("urlset"):
            return "sitemap"
    except Exception:
        pass
    return "sitemap"

# ------------------ extractors ------------------

def extract_links_from_html(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.find_all("a", href=True):
        links.append(normalize_url(base_url, a["href"]))
    return links

# naive extract from XML sitemap/index

def extract_from_sitemap(xml_text: str) -> Tuple[str, list[str]]:
    kind = sniff_sitemap_kind(xml_text)
    try:
        root = SafeET.fromstring(xml_text.encode("utf-8"))
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        if kind == "sitemap_index":
            x = [e.text for e in root.findall(".//sm:sitemap/sm:loc", ns) if e.text]
            return kind, x
        else:
            x = [e.text for e in root.findall(".//sm:url/sm:loc", ns) if e.text]
            return "sitemap", x
    except Exception:
        return kind, []
