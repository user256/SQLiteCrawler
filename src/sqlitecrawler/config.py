from __future__ import annotations
import os
import random
from dataclasses import dataclass
from urllib.parse import urlparse

DATA_DIR = os.getenv("SQLITECRAWLER_DATA", os.path.abspath("./data"))
os.makedirs(DATA_DIR, exist_ok=True)

@dataclass
class AuthConfig:
    """Authentication configuration for crawling protected sites."""
    username: str = ""
    password: str = ""
    auth_type: str = "basic"  # "basic", "digest", "bearer", "jwt", "api_key", "custom"
    domain: str = ""  # Optional: restrict auth to specific domain
    # Token-based authentication
    token: str = ""  # Bearer token, JWT token, or API key
    # Custom headers
    custom_headers: dict = None  # Custom header/value pairs

@dataclass
class HttpConfig:
    user_agent: str = os.getenv("SQLITECRAWLER_UA", "SQLiteCrawler/0.2 (+https://github.com/user256/SQLiteCrawler)")
    timeout: int = int(os.getenv("SQLITECRAWLER_TIMEOUT", "20"))
    max_concurrency: int = int(os.getenv("SQLITECRAWLER_CONCURRENCY", "5"))
    delay_between_requests: float = float(os.getenv("SQLITECRAWLER_DELAY", "0.2"))
    http_backend: str = os.getenv("SQLITECRAWLER_HTTP_BACKEND", "auto")
    curl_impersonate: str = os.getenv("SQLITECRAWLER_CURL_IMPERSONATE", "chrome120")
    respect_robots_txt: bool = os.getenv("SQLITECRAWLER_RESPECT_ROBOTS", "1") == "1"
    ignore_robots_crawlability: bool = False
    skip_robots_sitemaps: bool = False
    skip_sitemaps: bool = False
    # HTTP/2 and compression configuration
    enable_http2: bool = os.getenv("SQLITECRAWLER_HTTP2", "1") == "1"
    enable_brotli: bool = os.getenv("SQLITECRAWLER_BROTLI", "1") == "1"
    # Retry configuration
    max_retries: int = int(os.getenv("SQLITECRAWLER_MAX_RETRIES", "3"))
    retry_delay: float = float(os.getenv("SQLITECRAWLER_RETRY_DELAY", "1.0"))
    retry_backoff_factor: float = float(os.getenv("SQLITECRAWLER_RETRY_BACKOFF", "2.0"))
    # Politeness and rate limiting configuration
    enable_adaptive_delay: bool = os.getenv("SQLITECRAWLER_ADAPTIVE_DELAY", "1") == "1"
    min_delay: float = float(os.getenv("SQLITECRAWLER_MIN_DELAY", "0.1"))
    max_delay: float = float(os.getenv("SQLITECRAWLER_MAX_DELAY", "10.0"))
    delay_increase_factor: float = float(os.getenv("SQLITECRAWLER_DELAY_INCREASE", "1.5"))
    delay_decrease_factor: float = float(os.getenv("SQLITECRAWLER_DELAY_DECREASE", "0.9"))
    # Conditional requests configuration
    enable_conditional_requests: bool = os.getenv("SQLITECRAWLER_CONDITIONAL_REQUESTS", "1") == "1"
    # Cache TTL configuration
    robots_ttl: int = int(os.getenv("SQLITECRAWLER_ROBOTS_TTL", "86400"))  # 24 hours
    sitemap_ttl: int = int(os.getenv("SQLITECRAWLER_SITEMAP_TTL", "3600"))  # 1 hour
    # Authentication configuration
    auth: AuthConfig = None

    def __post_init__(self):
        self.http_backend = (self.http_backend or "auto").lower()
        self.curl_impersonate = self.curl_impersonate or "chrome120"

@dataclass
class CrawlLimits:
    max_pages: int = int(os.getenv("SQLITECRAWLER_MAX_PAGES", "0"))  # 0 = no limit
    max_depth: int = int(os.getenv("SQLITECRAWLER_MAX_DEPTH", "3"))
    same_host_only: bool = os.getenv("SQLITECRAWLER_SAME_HOST_ONLY", "1") == "1"

def get_website_db_name(url: str) -> str:
    """Extract domain from URL and create a safe database name by replacing dots with underscores."""
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    # Remove www. prefix if present
    if domain.startswith('www.'):
        domain = domain[4:]
    # Replace dots with underscores and remove any other problematic characters
    safe_name = domain.replace('.', '_').replace('-', '_')
    # Remove any remaining non-alphanumeric characters except underscores
    safe_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in safe_name)
    return safe_name

def get_db_paths(start_url: str) -> tuple[str, str]:
    """Get database paths based on the start URL."""
    website_name = get_website_db_name(start_url)
    pages_db = os.path.join(DATA_DIR, f"{website_name}_pages.db")
    crawl_db = os.path.join(DATA_DIR, f"{website_name}_crawl.db")
    return pages_db, crawl_db

# Default paths (will be overridden by get_db_paths)
PAGES_DB_PATH = os.path.join(DATA_DIR, "pages.db")
CRAWL_DB_PATH = os.path.join(DATA_DIR, "crawl.db")

# User agent strings for different scenarios
USER_AGENTS = {
    "default": "SQLiteCrawler/0.2 (+https://github.com/user256/SQLiteCrawler)",
    "chrome": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "firefox": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "safari": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "edge": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "mobile": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1"
}

def get_user_agent(ua_type: str = "default") -> str:
    """Get a user agent string by type or return a random one if 'random' is specified."""
    if ua_type == "random":
        return random.choice(list(USER_AGENTS.values()))
    return USER_AGENTS.get(ua_type, USER_AGENTS["default"])
