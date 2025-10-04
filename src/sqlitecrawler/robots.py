"""
Robots.txt parsing and sitemap discovery functionality.
"""
import aiohttp
import asyncio
from urllib.parse import urljoin, urlparse
from typing import List, Optional, Dict, Set
import urllib.robotparser
from bs4 import BeautifulSoup


class RobotsCache:
    """Cache for robots.txt files to avoid repeated requests."""
    
    def __init__(self):
        self._cache: Dict[str, urllib.robotparser.RobotFileParser] = {}
        self._failed_domains: Set[str] = set()
    
    def get_robots_parser(self, domain: str) -> Optional[urllib.robotparser.RobotFileParser]:
        """Get cached robots parser for domain."""
        return self._cache.get(domain)
    
    def set_robots_parser(self, domain: str, parser: urllib.robotparser.RobotFileParser):
        """Cache robots parser for domain."""
        self._cache[domain] = parser
    
    def mark_failed(self, domain: str):
        """Mark domain as failed to fetch robots.txt."""
        self._failed_domains.add(domain)
    
    def is_failed(self, domain: str) -> bool:
        """Check if domain failed to fetch robots.txt."""
        return domain in self._failed_domains


# Global robots cache
robots_cache = RobotsCache()


async def fetch_robots_txt(domain: str, user_agent: str = "SQLiteCrawler/0.2") -> Optional[str]:
    """Fetch robots.txt content for a domain."""
    robots_url = f"https://{domain}/robots.txt"
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(robots_url, headers={'User-Agent': user_agent}, timeout=10) as response:
                if response.status == 200:
                    return await response.text()
                elif response.status >= 500:
                    print(f"[robots.txt] Server error {response.status} for {robots_url}, assuming crawl allowed")
                    return None
                else:
                    print(f"[robots.txt] HTTP {response.status} for {robots_url}")
                    return None
    except Exception as e:
        print(f"[robots.txt] Error fetching {robots_url}: {e}")
        return None


async def parse_robots_txt(domain: str, user_agent: str = "SQLiteCrawler/0.2") -> Optional[urllib.robotparser.RobotFileParser]:
    """Parse robots.txt and return RobotFileParser object."""
    
    # Check cache first
    if robots_cache.is_failed(domain):
        return None
    
    cached_parser = robots_cache.get_robots_parser(domain)
    if cached_parser:
        return cached_parser
    
    # Fetch robots.txt
    robots_content = await fetch_robots_txt(domain, user_agent)
    if robots_content is None:
        robots_cache.mark_failed(domain)
        return None
    
    # Parse robots.txt
    try:
        parser = urllib.robotparser.RobotFileParser()
        parser.set_url(f"https://{domain}/robots.txt")
        
        # Initialize parser attributes
        parser._user_agents = []
        parser._entries = {}
        
        # Manually parse the content
        current_user_agent = None
        for line in robots_content.splitlines():
            line = line.strip()
            if line and not line.startswith('#'):
                # Parse each line manually
                if ':' in line:
                    key, value = line.split(':', 1)
                    key = key.strip().lower()
                    value = value.strip()
                    
                    if key == 'user-agent':
                        current_user_agent = value
                        if current_user_agent not in parser._user_agents:
                            parser._user_agents.append(current_user_agent)
                    elif key in ['disallow', 'allow'] and current_user_agent:
                        if current_user_agent not in parser._entries:
                            parser._entries[current_user_agent] = []
                        parser._entries[current_user_agent].append((key, value))
        
        # If no user-agent was specified, use '*' as default
        if not parser._user_agents:
            parser._user_agents = ['*']
            if '*' not in parser._entries:
                parser._entries['*'] = []
        
        # Cache the parser
        robots_cache.set_robots_parser(domain, parser)
        return parser
        
    except Exception as e:
        print(f"[robots.txt] Error parsing robots.txt for {domain}: {e}")
        robots_cache.mark_failed(domain)
        return None


def extract_sitemaps_from_robots(robots_content: str) -> List[str]:
    """Extract sitemap URLs from robots.txt content."""
    sitemaps = []
    
    for line in robots_content.splitlines():
        line = line.strip()
        if line.lower().startswith('sitemap:'):
            sitemap_url = line.split(':', 1)[1].strip()
            if sitemap_url:
                sitemaps.append(sitemap_url)
    
    return sitemaps


async def get_sitemaps_from_robots(domain: str, user_agent: str = "SQLiteCrawler/0.2") -> List[str]:
    """Get sitemap URLs from robots.txt for a domain."""
    robots_content = await fetch_robots_txt(domain, user_agent)
    if robots_content:
        return extract_sitemaps_from_robots(robots_content)
    return []


def is_url_crawlable(url: str, user_agent: str = "SQLiteCrawler/0.2") -> bool:
    """Check if a URL is crawlable according to robots.txt."""
    from urllib.parse import urlparse
    
    parsed = urlparse(url)
    domain = parsed.netloc
    path = parsed.path
    
    # Check cache first
    if robots_cache.is_failed(domain):
        return True  # Assume crawlable if robots.txt failed
    
    parser = robots_cache.get_robots_parser(domain)
    if parser is None:
        return True  # Assume crawlable if no robots.txt
    
    # Check if we have entries for this user agent or wildcard
    entries = parser._entries.get(user_agent, []) + parser._entries.get('*', [])
    
    # If no entries, allow crawling
    if not entries:
        return True
    
    # Check each rule
    for rule_type, rule_path in entries:
        if rule_type == 'disallow':
            # Check if the path matches the disallow pattern
            if rule_path == '/':
                return False  # Disallow everything
            elif rule_path.endswith('*'):
                # Wildcard pattern
                pattern = rule_path[:-1]  # Remove the *
                if path.startswith(pattern):
                    return False
            else:
                # Exact match
                if path.startswith(rule_path):
                    return False
        elif rule_type == 'allow':
            # Allow rules override disallow rules
            if rule_path == '/':
                return True  # Allow everything
            elif rule_path.endswith('*'):
                # Wildcard pattern
                pattern = rule_path[:-1]  # Remove the *
                if path.startswith(pattern):
                    return True
            else:
                # Exact match
                if path.startswith(rule_path):
                    return True
    
    # If no rules matched, allow by default
    return True


async def fetch_sitemap(url: str, user_agent: str = "SQLiteCrawler/0.2", verbose: bool = False) -> Optional[BeautifulSoup]:
    """Fetch and parse a sitemap XML."""
    if verbose:
        print(f"[sitemap] Fetching: {url}")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers={'User-Agent': user_agent}, timeout=30) as response:
                if verbose:
                    print(f"[sitemap] Response: {response.status} for {url}")
                
                if response.status == 200:
                    content = await response.text()
                    if verbose:
                        print(f"[sitemap] Content length: {len(content)} bytes")
                    return BeautifulSoup(content, 'xml')
                else:
                    print(f"[sitemap] HTTP {response.status} for {url}")
                    return None
    except Exception as e:
        print(f"[sitemap] Error fetching {url}: {e}")
        return None


def process_sitemap(sitemap_soup: BeautifulSoup, verbose: bool = False) -> tuple[List[str], Dict[str, Dict]]:
    """Process sitemap XML and return (sitemap_indexes, urls_dict)."""
    if not sitemap_soup:
        if verbose:
            print("[sitemap] No sitemap content to process")
        return [], {}
    
    sitemap_tags = sitemap_soup.find_all("sitemap")
    url_tags = sitemap_soup.find_all("url")
    
    if verbose:
        print(f"[sitemap] Found {len(sitemap_tags)} sitemap tags, {len(url_tags)} URL tags")
    
    if sitemap_tags:
        # This is a sitemap index
        sitemap_indexes = []
        for tag in sitemap_tags:
            loc_tag = tag.find("loc")
            if loc_tag and loc_tag.text:
                sitemap_url = loc_tag.text.strip()
                sitemap_indexes.append(sitemap_url)
                if verbose:
                    print(f"[sitemap] Found nested sitemap: {sitemap_url}")
        
        if verbose:
            print(f"[sitemap] Total nested sitemaps: {len(sitemap_indexes)}")
        return sitemap_indexes, {}
    
    elif url_tags:
        # This is a regular sitemap
        urls_dict = {}
        hreflang_count = 0
        
        for url_element in url_tags:
            loc_tag = url_element.find("loc")
            if not loc_tag or not loc_tag.text:
                continue
            
            url_value = loc_tag.text.strip()
            
            # Extract hreflang data
            hreflangs = []
            hrefs = []
            for link in url_element.find_all('xhtml:link'):
                hreflang = link.get('hreflang')
                href = link.get('href')
                if hreflang:
                    hreflangs.append(hreflang)
                if href:
                    hrefs.append(href)
            
            if hreflangs:
                hreflang_count += len(hreflangs)
            
            urls_dict[url_value] = {
                'hreflangs': hreflangs,
                'hrefs': hrefs
            }
        
        if verbose:
            print(f"[sitemap] Processed {len(urls_dict)} URLs with {hreflang_count} total hreflang entries")
        
        return [], urls_dict
    
    if verbose:
        print("[sitemap] No sitemap or URL tags found")
    return [], {}


async def crawl_sitemaps_recursive(sitemap_urls: List[str], user_agent: str = "SQLiteCrawler/0.2", verbose: bool = False) -> tuple[Dict[str, Dict], Dict[str, str]]:
    """Recursively crawl sitemap URLs and extract all URLs.
    Returns (urls_dict, url_to_sitemap_mapping) where url_to_sitemap_mapping maps each URL to its source sitemap.
    """
    crawled = set()
    all_urls = {}
    url_to_sitemap = {}  # Maps URL to the sitemap it was found in
    
    while sitemap_urls:
        current_sitemap = sitemap_urls.pop(0)  # Use pop(0) for FIFO
        
        if current_sitemap in crawled:
            if verbose:
                print(f"[sitemap] Skipping already processed: {current_sitemap}")
            continue
        
        print(f"[sitemap] Processing: {current_sitemap}")
        
        sitemap_soup = await fetch_sitemap(current_sitemap, user_agent, verbose)
        if sitemap_soup:
            nested_indexes, new_urls = process_sitemap(sitemap_soup, verbose)
            
            # Add new URLs and track which sitemap they came from
            for url in new_urls.keys():
                all_urls[url] = new_urls[url]
                url_to_sitemap[url] = current_sitemap
            
            # Add nested sitemap indexes to queue
            if nested_indexes:
                sitemap_urls.extend(nested_indexes)
                print(f"[sitemap] Found {len(nested_indexes)} nested sitemaps")
        else:
            if verbose:
                print(f"[sitemap] Failed to fetch or parse: {current_sitemap}")
        
        crawled.add(current_sitemap)
        print(f"[sitemap] Total URLs discovered so far: {len(all_urls)}")
    
    return all_urls, url_to_sitemap


async def discover_sitemaps_from_domain(domain: str, user_agent: str = "SQLiteCrawler/0.2", skip_robots: bool = False) -> List[str]:
    """Discover all sitemaps for a domain starting from robots.txt."""
    initial_sitemaps = []
    
    if not skip_robots:
        # Get sitemaps from robots.txt
        initial_sitemaps = await get_sitemaps_from_robots(domain, user_agent)
    
    if not initial_sitemaps:
        # Try common sitemap locations
        common_sitemaps = [
            f"https://{domain}/sitemap.xml",
            f"https://{domain}/sitemap_index.xml",
            f"https://{domain}/sitemaps.xml"
        ]
        
        # Test which ones exist
        for sitemap_url in common_sitemaps:
            sitemap_soup = await fetch_sitemap(sitemap_url, user_agent)
            if sitemap_soup:
                initial_sitemaps.append(sitemap_url)
                break
    
    return initial_sitemaps
