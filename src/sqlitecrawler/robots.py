"""
Robots.txt parsing and sitemap discovery functionality.
"""
import aiohttp
import asyncio
import time
from urllib.parse import urljoin, urlparse
from typing import List, Optional, Dict, Set, Tuple
import urllib.robotparser
from bs4 import BeautifulSoup
from email.utils import parsedate_to_datetime


def calculate_cache_ttl(headers: Dict[str, str], default_ttl: int = 3600) -> int:
    """Calculate cache TTL from server headers, respecting Cache-Control and Expires."""
    # TODO: This could be more sophisticated - maybe respect ETag-based caching too?
    # FIXME: The current logic doesn't handle all Cache-Control directives properly
    # NOTE: This is a simplified implementation - real caching is more complex
    try:
        # Check Cache-Control header first
        cache_control = headers.get('cache-control', '').lower()
        if cache_control:
            # Parse max-age directive
            if 'max-age=' in cache_control:
                max_age_str = cache_control.split('max-age=')[1].split(',')[0].strip()
                try:
                    max_age = int(max_age_str)
                    return max_age
                except ValueError:
                    pass
            
        # Check for no-cache or no-store
        if 'no-cache' in cache_control or 'no-store' in cache_control:
            return 0  # Don't cache at all
        
        # Check Expires header
        expires = headers.get('expires')
        if expires:
            try:
                expires_dt = parsedate_to_datetime(expires)
                current_dt = time.time()
                expires_ts = expires_dt.timestamp()
                ttl = int(expires_ts - current_dt)
                return max(0, ttl)  # Don't return negative TTL
            except (ValueError, TypeError):
                pass
        
        # Check Last-Modified for heuristic freshness
        last_modified = headers.get('last-modified')
        if last_modified:
            try:
                # Use 10% of age as freshness (RFC 7234)
                last_modified_dt = parsedate_to_datetime(last_modified)
                age = time.time() - last_modified_dt.timestamp()
                heuristic_ttl = int(age * 0.1)
                return max(0, min(heuristic_ttl, default_ttl))
            except (ValueError, TypeError):
                pass
        
        return default_ttl
    except Exception:
        return default_ttl


class RobotsCache:
    """Cache for robots.txt files with server cache-aware TTL support."""
    
    def __init__(self, default_ttl: int = 86400):  # 24 hours default TTL
        self._cache: Dict[str, Tuple[urllib.robotparser.RobotFileParser, float, Dict[str, float], Dict[str, str]]] = {}
        self._failed_domains: Set[str] = set()
        self._default_ttl = default_ttl
    
    def get_robots_parser(self, domain: str) -> Optional[urllib.robotparser.RobotFileParser]:
        """Get cached robots parser for domain if not expired."""
        if domain not in self._cache:
            return None
        
        parser, cached_time, crawl_delays, headers = self._cache[domain]
        current_time = time.time()
        
        # Calculate TTL from server headers
        server_ttl = calculate_cache_ttl(headers, self._default_ttl)
        
        # Check if cache entry has expired
        if current_time - cached_time > server_ttl:
            del self._cache[domain]
            return None
        
        return parser
    
    def get_crawl_delay(self, domain: str, user_agent: str = "*") -> Optional[float]:
        """Get crawl delay for domain and user agent if not expired."""
        if domain not in self._cache:
            return None
        
        parser, cached_time, crawl_delays, headers = self._cache[domain]
        current_time = time.time()
        
        # Calculate TTL from server headers
        server_ttl = calculate_cache_ttl(headers, self._default_ttl)
        
        # Check if cache entry has expired
        if current_time - cached_time > server_ttl:
            del self._cache[domain]
            return None
        
        # Return crawl delay for specific user agent or wildcard
        return crawl_delays.get(user_agent) or crawl_delays.get("*")
    
    def set_robots_parser(self, domain: str, parser: urllib.robotparser.RobotFileParser, crawl_delays: Dict[str, float] = None, headers: Dict[str, str] = None):
        """Cache robots parser for domain with TTL."""
        current_time = time.time()
        self._cache[domain] = (parser, current_time, crawl_delays or {}, headers or {})
    
    def mark_failed(self, domain: str):
        """Mark domain as failed to fetch robots.txt."""
        self._failed_domains.add(domain)
    
    def is_failed(self, domain: str) -> bool:
        """Check if domain failed to fetch robots.txt."""
        return domain in self._failed_domains
    
    def clear_expired(self):
        """Clear expired cache entries."""
        current_time = time.time()
        expired_domains = []
        
        for domain, (parser, cached_time, crawl_delays, headers) in self._cache.items():
            server_ttl = calculate_cache_ttl(headers, self._default_ttl)
            if current_time - cached_time > server_ttl:
                expired_domains.append(domain)
        
        for domain in expired_domains:
            del self._cache[domain]


class SitemapCache:
    """Cache for sitemap content with server cache-aware TTL support."""
    
    def __init__(self, default_ttl: int = 3600):  # 1 hour default TTL for sitemaps
        self._cache: Dict[str, Tuple[BeautifulSoup, float, Dict[str, str]]] = {}
        self._failed_sitemaps: Set[str] = set()
        self._default_ttl = default_ttl
    
    def get_sitemap(self, sitemap_url: str) -> Optional[BeautifulSoup]:
        """Get cached sitemap content if not expired."""
        if sitemap_url not in self._cache:
            return None
        
        sitemap_soup, cached_time, headers = self._cache[sitemap_url]
        current_time = time.time()
        
        # Calculate TTL from server headers
        server_ttl = calculate_cache_ttl(headers, self._default_ttl)
        
        # Check if cache entry has expired
        if current_time - cached_time > server_ttl:
            del self._cache[sitemap_url]
            return None
        
        return sitemap_soup
    
    def set_sitemap(self, sitemap_url: str, sitemap_soup: BeautifulSoup, headers: Dict[str, str] = None):
        """Cache sitemap content with TTL."""
        current_time = time.time()
        self._cache[sitemap_url] = (sitemap_soup, current_time, headers or {})
    
    def mark_failed(self, sitemap_url: str):
        """Mark sitemap as failed to fetch."""
        self._failed_sitemaps.add(sitemap_url)
    
    def is_failed(self, sitemap_url: str) -> bool:
        """Check if sitemap failed to fetch."""
        return sitemap_url in self._failed_sitemaps
    
    def clear_expired(self):
        """Clear expired cache entries."""
        current_time = time.time()
        expired_sitemaps = []
        
        for sitemap_url, (sitemap_soup, cached_time, headers) in self._cache.items():
            server_ttl = calculate_cache_ttl(headers, self._default_ttl)
            if current_time - cached_time > server_ttl:
                expired_sitemaps.append(sitemap_url)
        
        for sitemap_url in expired_sitemaps:
            del self._cache[sitemap_url]

# Global caches (will be initialized with config values)
robots_cache = None
sitemap_cache = None

def init_caches(http_config=None):
    """Initialize global caches with config values."""
    global robots_cache, sitemap_cache
    
    if http_config:
        robots_ttl = getattr(http_config, 'robots_ttl', 86400)
        sitemap_ttl = getattr(http_config, 'sitemap_ttl', 3600)
    else:
        robots_ttl = 86400  # 24 hours default
        sitemap_ttl = 3600  # 1 hour default
    
    robots_cache = RobotsCache(robots_ttl)
    sitemap_cache = SitemapCache(sitemap_ttl)


async def fetch_robots_txt(domain: str, user_agent: str = "SQLiteCrawler/0.2", http_config=None) -> Tuple[Optional[str], Dict[str, str]]:
    """Fetch robots.txt content for a domain and return content with headers."""
    robots_url = f"https://{domain}/robots.txt"
    
    # Prepare authentication if needed
    auth = None
    if http_config and http_config.auth:
        from .fetch import _should_use_auth, _create_auth
        if _should_use_auth(robots_url, http_config.auth):
            auth = _create_auth(http_config.auth)
    
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(robots_url, headers={'User-Agent': user_agent}, auth=auth) as response:
                headers = dict(response.headers)
                if response.status == 200:
                    content = await response.text()
                    return content, headers
                elif response.status >= 500:
                    print(f"[robots.txt] Server error {response.status} for {robots_url}, assuming crawl allowed")
                    return None, headers
                else:
                    print(f"[robots.txt] HTTP {response.status} for {robots_url}")
                    return None, headers
    except Exception as e:
        print(f"[robots.txt] Error fetching {robots_url}: {e}")
        return None, {}


async def parse_robots_txt(domain: str, user_agent: str = "SQLiteCrawler/0.2", http_config=None) -> Optional[urllib.robotparser.RobotFileParser]:
    """Parse robots.txt and return RobotFileParser object."""
    
    # Initialize caches if not already done
    if robots_cache is None:
        init_caches(http_config)
    
    # Check cache first
    if robots_cache.is_failed(domain):
        return None
    
    cached_parser = robots_cache.get_robots_parser(domain)
    if cached_parser:
        return cached_parser
    
    # Fetch robots.txt
    robots_content, headers = await fetch_robots_txt(domain, user_agent, http_config)
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
        crawl_delays = {}  # user_agent -> delay in seconds
        
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
                    elif key == 'crawl-delay' and current_user_agent:
                        try:
                            delay = float(value)
                            crawl_delays[current_user_agent] = delay
                        except ValueError:
                            print(f"[robots.txt] Invalid crawl-delay value '{value}' for user-agent '{current_user_agent}'")
        
        # If no user-agent was specified, use '*' as default
        if not parser._user_agents:
            parser._user_agents = ['*']
            if '*' not in parser._entries:
                parser._entries['*'] = []
        
        # Cache the parser with crawl delays and headers
        robots_cache.set_robots_parser(domain, parser, crawl_delays, headers)
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


async def get_sitemaps_from_robots(domain: str, user_agent: str = "SQLiteCrawler/0.2", http_config=None) -> List[str]:
    """Get sitemap URLs from robots.txt for a domain."""
    robots_content, headers = await fetch_robots_txt(domain, user_agent, http_config)
    if robots_content:
        return extract_sitemaps_from_robots(robots_content)
    return []


def get_crawl_delay(domain: str, user_agent: str = "SQLiteCrawler/0.2") -> Optional[float]:
    """Get crawl delay for domain and user agent from robots.txt."""
    if robots_cache is None:
        return None
    return robots_cache.get_crawl_delay(domain, user_agent)

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


async def fetch_sitemap(url: str, user_agent: str = "SQLiteCrawler/0.2", verbose: bool = False, http_config=None) -> Optional[BeautifulSoup]:
    """Fetch and parse a sitemap XML with caching support."""
    
    # Initialize caches if not already done
    if sitemap_cache is None:
        init_caches(http_config)
    
    # Check cache first
    if sitemap_cache.is_failed(url):
        if verbose:
            print(f"[sitemap] Skipping failed sitemap: {url}")
        return None
    
    cached_sitemap = sitemap_cache.get_sitemap(url)
    if cached_sitemap:
        if verbose:
            print(f"[sitemap] Using cached sitemap: {url}")
        return cached_sitemap
    
    if verbose:
        print(f"[sitemap] Fetching: {url}")
    
    # Prepare authentication if needed
    auth = None
    if http_config and http_config.auth:
        from .fetch import _should_use_auth, _create_auth
        if _should_use_auth(url, http_config.auth):
            auth = _create_auth(http_config.auth)
    
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers={'User-Agent': user_agent}, auth=auth) as response:
                if verbose:
                    print(f"[sitemap] Response: {response.status} for {url}")
                
                if response.status == 200:
                    content = await response.text()
                    if verbose:
                        print(f"[sitemap] Content length: {len(content)} bytes")
                    sitemap_soup = BeautifulSoup(content, 'xml')
                    headers = dict(response.headers)
                    
                    # Cache the successful result with headers
                    sitemap_cache.set_sitemap(url, sitemap_soup, headers)
                    return sitemap_soup
                else:
                    print(f"[sitemap] HTTP {response.status} for {url}")
                    sitemap_cache.mark_failed(url)
                    return None
    except Exception as e:
        print(f"[sitemap] Error fetching {url}: {e}")
        sitemap_cache.mark_failed(url)
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
            
            # Extract priority and lastmod
            priority_tag = url_element.find("priority")
            priority = None
            if priority_tag and priority_tag.text:
                try:
                    priority = float(priority_tag.text.strip())
                except ValueError:
                    priority = None
            
            lastmod_tag = url_element.find("lastmod")
            lastmod = None
            if lastmod_tag and lastmod_tag.text:
                lastmod = lastmod_tag.text.strip()
            
            urls_dict[url_value] = {
                'hreflangs': hreflangs,
                'hrefs': hrefs,
                'priority': priority,
                'lastmod': lastmod
            }
        
        if verbose:
            print(f"[sitemap] Processed {len(urls_dict)} URLs with {hreflang_count} total hreflang entries")
        
        return [], urls_dict
    
    if verbose:
        print("[sitemap] No sitemap or URL tags found")
    return [], {}


async def crawl_sitemaps_recursive(sitemap_urls: List[str], user_agent: str = "SQLiteCrawler/0.2", verbose: bool = False, http_config=None) -> tuple[Dict[str, Dict], Dict[str, str]]:
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
        
        sitemap_soup = await fetch_sitemap(current_sitemap, user_agent, verbose, http_config)
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


async def discover_sitemaps_from_domain(domain: str, user_agent: str = "SQLiteCrawler/0.2", skip_robots: bool = False, http_config=None) -> List[str]:
    """Discover all sitemaps for a domain starting from robots.txt."""
    initial_sitemaps = []
    
    if not skip_robots:
        # Get sitemaps from robots.txt
        initial_sitemaps = await get_sitemaps_from_robots(domain, user_agent, http_config)
    
    if not initial_sitemaps:
        # Try common sitemap locations
        common_sitemaps = [
            f"https://{domain}/sitemap.xml",
            f"https://{domain}/sitemap_index.xml",
            f"https://{domain}/sitemaps.xml"
        ]
        
        # Test which ones exist
        for sitemap_url in common_sitemaps:
            sitemap_soup = await fetch_sitemap(sitemap_url, user_agent, False, http_config)
            if sitemap_soup:
                initial_sitemaps.append(sitemap_url)
                break
    
    return initial_sitemaps
