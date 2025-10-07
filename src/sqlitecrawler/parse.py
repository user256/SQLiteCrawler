from __future__ import annotations
from urllib.parse import urljoin, urlsplit, urlunsplit, urlparse, parse_qs, urlencode
from typing import Iterable, Tuple
from bs4 import BeautifulSoup
from defusedxml import ElementTree as SafeET
import idna

# ------------------ URL helpers ------------------

def normalize_url(base: str, href: str) -> str:
    u = urljoin(base, href)
    parts = list(urlsplit(u))
    # keep query; drop fragment
    parts[4] = ""
    return urlunsplit(parts)

def normalize_url_hardened(url: str) -> str:
    """
    Comprehensive URL normalization with hardening:
    - Punycode normalization
    - Default port stripping
    - Parameter sorting
    - UTM parameter stripping
    """
    parsed = urlparse(url)
    
    # 1. Normalize scheme (lowercase)
    scheme = parsed.scheme.lower()
    
    # 2. Normalize domain (punycode + lowercase)
    try:
        # Convert domain to punycode for international domains
        domain = idna.encode(parsed.netloc.lower()).decode('ascii')
    except (idna.IDNAError, UnicodeError):
        # Fallback to lowercase if punycode conversion fails
        domain = parsed.netloc.lower()
    
    # 3. Strip default ports
    if parsed.port:
        default_ports = {'http': 80, 'https': 443, 'ftp': 21, 'ftps': 990}
        if parsed.port == default_ports.get(scheme):
            # Remove port from domain
            domain = domain.rsplit(':', 1)[0]
    
    # 4. Normalize path (preserve trailing slashes - they matter for server behavior)
    path = parsed.path
    # Don't remove trailing slashes - they can be significant for server routing
    
    # 5. Handle query parameters
    query = parsed.query
    if query:
        # Parse and filter UTM parameters
        params = parse_qs(query, keep_blank_values=True)
        
        # UTM parameters to remove
        utm_params = {
            'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 
            'utm_content', 'utm_id', 'utm_source_platform', 
            'utm_creative_format', 'utm_marketing_tactic'
        }
        
        # Remove UTM parameters
        filtered_params = {k: v for k, v in params.items() 
                          if k.lower() not in utm_params}
        
        # Sort parameters alphabetically
        sorted_params = sorted(filtered_params.items())
        query = urlencode(sorted_params, doseq=True) if sorted_params else ""
    
    # 6. Normalize fragment (lowercase)
    fragment = parsed.fragment.lower() if parsed.fragment else ""
    
    return urlunsplit((scheme, domain, path, query, fragment))

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

def extract_links_with_metadata(html: str, base_url: str) -> tuple[list[str], list[dict]]:
    """
    Extract links with anchor text and xpath metadata.
    Returns (simple_links_list, detailed_links_list)
    """
    soup = BeautifulSoup(html, "lxml")
    simple_links = []
    detailed_links = []
    
    for a in soup.find_all("a", href=True):
        href = a["href"]
        
        # 1. ORIGINAL href (preserve everything for analysis)
        original_href = urljoin(base_url, href)
        
        # 2. NORMALIZED href (for crawling decisions - use hardened normalization)
        normalized_url = normalize_url_hardened(original_href)
        
        # 3. Extract original components for analysis
        parsed_original = urlparse(original_href)
        original_fragment = parsed_original.fragment
        original_params = parsed_original.query
        
        simple_links.append(normalized_url)
        
        # Extract anchor text (strip whitespace)
        anchor_text = a.get_text(strip=True)
        
        # If anchor text is empty, try to extract alt text from images
        if not anchor_text:
            # Look for images within the link (including nested ones)
            img = a.find("img")
            if img:
                alt_text = img.get("alt", "").strip()
                if alt_text:
                    anchor_text = f"[IMG: {alt_text}]"
                else:
                    # Image has no alt text, try to use src filename as fallback
                    src = img.get("src", "")
                    if src:
                        # Extract filename from src
                        import os
                        filename = os.path.basename(src)
                        if filename:
                            anchor_text = f"[IMG: {filename}]"
        
        # If still no anchor text, try to extract title attribute
        if not anchor_text and a.get("title"):
            anchor_text = a.get("title").strip()
            if anchor_text:
                anchor_text = f"[TITLE: {anchor_text}]"
        
        # If still no anchor text, try to extract aria-label
        if not anchor_text and a.get("aria-label"):
            anchor_text = a.get("aria-label").strip()
            if anchor_text:
                anchor_text = f"[ARIA: {anchor_text}]"
        
        # If still no anchor text, try to extract data attributes that might contain text
        if not anchor_text:
            for attr in ["data-text", "data-label", "data-name", "data-action", "data-target"]:
                if a.get(attr):
                    anchor_text = a.get(attr).strip()
                    if anchor_text:
                        anchor_text = f"[DATA: {anchor_text}]"
                        break
        
        # If still no anchor text, try to extract class names that might be meaningful
        if not anchor_text and a.get("class"):
            classes = a.get("class")
            if isinstance(classes, list):
                classes = " ".join(classes)
            # Look for meaningful class names
            meaningful_classes = []
            for cls in classes.split():
                if any(keyword in cls.lower() for keyword in ["skip", "back", "top", "close", "menu", "nav", "button", "link"]):
                    meaningful_classes.append(cls)
            if meaningful_classes:
                anchor_text = f"[CLASS: {' '.join(meaningful_classes)}]"
        
        # If still no anchor text, try to extract id attribute
        if not anchor_text and a.get("id"):
            anchor_text = f"[ID: {a.get('id')}]"
        
        # If still no anchor text, try to extract href fragment for anchor links
        if not anchor_text and href.startswith("#"):
            anchor_text = f"[ANCHOR: {href[1:]}]"
        
        # If still no anchor text, try to extract meaningful href path
        if not anchor_text and href and not href.startswith("javascript:"):
            # Extract the last meaningful part of the path
            import os
            path_parts = href.split("/")
            if path_parts:
                last_part = path_parts[-1]
                if last_part and not last_part.startswith("#"):
                    # Clean up the path part
                    clean_part = last_part.replace("-", " ").replace("_", " ").title()
                    if clean_part and len(clean_part) > 2:
                        anchor_text = f"[PATH: {clean_part}]"
        
        # Generate xpath for the link element
        xpath = generate_xpath(a)
        
        detailed_links.append({
            "url": normalized_url,                    # Normalized URL for crawling
            "original_href": original_href,           # Original href for analysis
            "anchor_text": anchor_text,
            "xpath": xpath,
            "fragment": original_fragment,            # Original fragment
            "parameters": original_params,            # Original parameters
            "href": href                              # Original relative href
        })
    
    return simple_links, detailed_links

def generate_xpath(element) -> str:
    """Generate xpath for a BeautifulSoup element."""
    path = []
    current = element
    
    while current and current.name:
        # Get tag name
        tag = current.name
        
        # Check if we need to add position index
        if current.parent:
            siblings = [s for s in current.parent.find_all(tag, recursive=False) if s.name == tag]
            if len(siblings) > 1:
                position = siblings.index(current) + 1
                tag = f"{tag}[{position}]"
        
        path.insert(0, tag)
        current = current.parent
    
    return "/" + "/".join(path) if path else ""

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
