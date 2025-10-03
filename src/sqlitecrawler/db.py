from __future__ import annotations
import aiosqlite, json, zlib, base64, time, asyncio
from typing import Optional, Iterable, Tuple, List, Dict, Any
from .config import PAGES_DB_PATH, CRAWL_DB_PATH

# ------------------ compression helpers ------------------

def compress_html(html: str) -> bytes:
    return base64.b64encode(zlib.compress(html.encode("utf-8")))

def decompress_html(encoded: bytes) -> str:
    try:
        return zlib.decompress(base64.b64decode(encoded)).decode("utf-8")
    except Exception:
        try:
            return encoded.decode("utf-8")  # type: ignore[arg-type]
        except Exception:
            return ""

def compress_headers(headers: dict) -> bytes:
    """Compress headers dictionary to bytes."""
    return base64.b64encode(zlib.compress(json.dumps(headers, ensure_ascii=False).encode("utf-8")))

def decompress_headers(encoded: bytes) -> dict:
    """Decompress headers from bytes to dictionary."""
    try:
        return json.loads(zlib.decompress(base64.b64decode(encoded)).decode("utf-8"))
    except Exception:
        return {}

def extract_content_from_html(html: str, headers: dict = None) -> dict:
    """Extract title, meta description, robots, canonical, h1, h2 tags and word count from HTML."""
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extract title
        title_tag = soup.find('title')
        title = title_tag.get_text().strip() if title_tag else None
        
        # Extract meta description
        meta_desc_tag = soup.find('meta', attrs={'name': 'description'})
        meta_description = meta_desc_tag.get('content', '').strip() if meta_desc_tag else None
        
        # Extract meta robots
        meta_robots_tag = soup.find('meta', attrs={'name': 'robots'})
        meta_robots = meta_robots_tag.get('content', '').strip() if meta_robots_tag else None
        
        # Extract canonical URL from HTML head
        canonical_tag = soup.find('link', attrs={'rel': 'canonical'})
        canonical_url = canonical_tag.get('href', '').strip() if canonical_tag else None
        
        # Extract HTML lang declaration
        html_tag = soup.find('html')
        html_lang = html_tag.get('lang', '').strip() if html_tag else None
        
        # Extract h1 tags
        h1_tags = [h1.get_text().strip() for h1 in soup.find_all('h1') if h1.get_text().strip()]
        
        # Extract h2 tags
        h2_tags = [h2.get_text().strip() for h2 in soup.find_all('h2') if h2.get_text().strip()]
        
        # Count words in visible text
        for script in soup(["script", "style"]):
            script.decompose()
        text = soup.get_text()
        words = text.split()
        word_count = len(words)
        
        # Parse robots directives from HTML meta
        html_meta_directives = []
        if meta_robots:
            directives = [d.strip().lower() for d in meta_robots.split(',')]
            html_meta_directives = directives
        
        # Parse robots directives from HTTP headers
        http_header_directives = []
        if headers:
            robots_header = headers.get('x-robots-tag', '')
            if robots_header:
                directives = [d.strip().lower() for d in robots_header.split(',')]
                http_header_directives = directives
        
        return {
            'title': title,
            'meta_description': meta_description,
            'h1_tags': h1_tags,
            'h2_tags': h2_tags,
            'word_count': word_count,
            'html_meta_directives': html_meta_directives,
            'http_header_directives': http_header_directives,
            'canonical_url': canonical_url,
            'html_lang': html_lang
        }
    except Exception as e:
        print(f"Error extracting content from HTML: {e}")
        return {
            'title': None,
            'meta_description': None,
            'h1_tags': [],
            'h2_tags': [],
            'word_count': 0,
            'html_meta_directives': [],
            'http_header_directives': [],
            'canonical_url': None,
            'html_lang': None
        }

# ------------------ database connection pool ------------------

class DatabasePool:
    """Async database connection pool for better performance."""
    
    def __init__(self, db_path: str, pool_size: int = 5):
        self.db_path = db_path
        self.pool_size = pool_size
        self._pool: List[aiosqlite.Connection] = []
        self._available: asyncio.Queue = asyncio.Queue()
        self._initialized = False
    
    async def initialize(self):
        """Initialize the connection pool."""
        if self._initialized:
            return
        
        # Use a single connection for now to avoid I/O conflicts
        conn = await aiosqlite.connect(self.db_path)
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=NORMAL")
        await conn.execute("PRAGMA cache_size=10000")
        await conn.execute("PRAGMA temp_store=MEMORY")
        self._pool.append(conn)
        await self._available.put(conn)
        
        self._initialized = True
    
    async def get_connection(self) -> aiosqlite.Connection:
        """Get a connection from the pool."""
        if not self._initialized:
            await self.initialize()
        return await self._available.get()
    
    async def return_connection(self, conn: aiosqlite.Connection):
        """Return a connection to the pool."""
        await self._available.put(conn)
    
    async def close(self):
        """Close all connections in the pool."""
        for conn in self._pool:
            await conn.close()
        self._pool.clear()
        self._initialized = False

# Global connection pools
_pages_pools: Dict[str, DatabasePool] = {}
_crawl_pools: Dict[str, DatabasePool] = {}

async def get_pages_pool(db_path: str) -> DatabasePool:
    """Get or create a pages database pool."""
    if db_path not in _pages_pools:
        _pages_pools[db_path] = DatabasePool(db_path)
        await _pages_pools[db_path].initialize()
    return _pages_pools[db_path]

async def get_crawl_pool(db_path: str) -> DatabasePool:
    """Get or create a crawl database pool."""
    if db_path not in _crawl_pools:
        _crawl_pools[db_path] = DatabasePool(db_path)
        await _crawl_pools[db_path].initialize()
    return _crawl_pools[db_path]

# ------------------ schema init ------------------

PAGES_SCHEMA = """
CREATE TABLE IF NOT EXISTS pages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url_id INTEGER NOT NULL,
  final_url_id INTEGER,
  status INTEGER,
  fetched_at INTEGER,
  headers_json TEXT,
  html_compressed BLOB,
  FOREIGN KEY (url_id) REFERENCES urls (id),
  FOREIGN KEY (final_url_id) REFERENCES urls (id),
  UNIQUE(url_id)
);
CREATE INDEX IF NOT EXISTS idx_pages_url_id ON pages(url_id);
"""

CRAWL_SCHEMA = """
-- Central URLs table with auto-incrementing IDs
CREATE TABLE IF NOT EXISTS urls (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url TEXT UNIQUE NOT NULL,
  kind TEXT CHECK (kind IN ('html','sitemap','sitemap_index','image','asset','other')),
  classification TEXT CHECK (classification IN ('internal','network','external','social')),
  discovered_from_id INTEGER,
  first_seen INTEGER,
  last_seen INTEGER,
  headers_compressed BLOB,
  FOREIGN KEY (discovered_from_id) REFERENCES urls (id)
);
CREATE INDEX IF NOT EXISTS idx_urls_url ON urls(url);
CREATE INDEX IF NOT EXISTS idx_urls_kind ON urls(kind);
CREATE INDEX IF NOT EXISTS idx_urls_classification ON urls(classification);

-- Content extraction table
CREATE TABLE IF NOT EXISTS content (
  url_id INTEGER PRIMARY KEY,
  title TEXT,
  meta_description TEXT,
  h1_tags TEXT,  -- JSON array of h1 texts
  h2_tags TEXT,  -- JSON array of h2 texts
  word_count INTEGER,
  html_lang TEXT,  -- HTML lang attribute
  internal_links_count INTEGER DEFAULT 0,
  external_links_count INTEGER DEFAULT 0,
  internal_links_unique_count INTEGER DEFAULT 0,
  external_links_unique_count INTEGER DEFAULT 0,
  crawl_depth INTEGER DEFAULT 0,
  inlinks_count INTEGER DEFAULT 0,
  inlinks_unique_count INTEGER DEFAULT 0,
  FOREIGN KEY (url_id) REFERENCES urls (id)
);
CREATE INDEX IF NOT EXISTS idx_content_url_id ON content(url_id);

-- Normalized anchor text table
CREATE TABLE IF NOT EXISTS anchor_texts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  text TEXT UNIQUE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_anchor_texts_text ON anchor_texts(text);

-- Normalized xpath table  
CREATE TABLE IF NOT EXISTS xpaths (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  xpath TEXT UNIQUE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_xpaths_xpath ON xpaths(xpath);

-- Normalized href table (relative and absolute paths)
CREATE TABLE IF NOT EXISTS hrefs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  href TEXT UNIQUE NOT NULL,  -- The actual href path
  is_absolute BOOLEAN NOT NULL  -- Whether this href is absolute or relative
);
CREATE INDEX IF NOT EXISTS idx_hrefs_href ON hrefs(href);
CREATE INDEX IF NOT EXISTS idx_hrefs_absolute ON hrefs(is_absolute);

-- Internal links table with fully normalized references
CREATE TABLE IF NOT EXISTS internal_links (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_url_id INTEGER NOT NULL,
  target_url_id INTEGER,  -- NULL if target doesn't exist in our DB yet
  anchor_text_id INTEGER,  -- Reference to normalized anchor text
  xpath_id INTEGER,  -- Reference to normalized xpath
  href_id INTEGER NOT NULL,  -- Reference to normalized href
  url_fragment TEXT,  -- #fragment part (only if present)
  url_parameters TEXT,  -- ?param=value part (only if present)
  discovered_at INTEGER NOT NULL,
  FOREIGN KEY (source_url_id) REFERENCES urls (id),
  FOREIGN KEY (target_url_id) REFERENCES urls (id),
  FOREIGN KEY (anchor_text_id) REFERENCES anchor_texts (id),
  FOREIGN KEY (xpath_id) REFERENCES xpaths (id),
  FOREIGN KEY (href_id) REFERENCES hrefs (id),
  UNIQUE(source_url_id, xpath_id)  -- Prevent duplicate links with same xpath
);
CREATE INDEX IF NOT EXISTS idx_internal_links_source ON internal_links(source_url_id);
CREATE INDEX IF NOT EXISTS idx_internal_links_target ON internal_links(target_url_id);
CREATE INDEX IF NOT EXISTS idx_internal_links_anchor ON internal_links(anchor_text_id);
CREATE INDEX IF NOT EXISTS idx_internal_links_xpath ON internal_links(xpath_id);

-- Normalized robots directive strings table
CREATE TABLE IF NOT EXISTS robots_directive_strings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  directive TEXT UNIQUE NOT NULL  -- e.g., 'noindex', 'nofollow', 'noarchive'
);
CREATE INDEX IF NOT EXISTS idx_robots_directive_strings_directive ON robots_directive_strings(directive);

-- Robots directives table (normalized)
CREATE TABLE IF NOT EXISTS robots_directives (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url_id INTEGER NOT NULL,
  source TEXT CHECK (source IN ('robots_txt', 'html_meta', 'http_header')) NOT NULL,
  directive_id INTEGER NOT NULL,  -- Reference to normalized directive
  value TEXT,  -- directive value if applicable
  FOREIGN KEY (url_id) REFERENCES urls (id),
  FOREIGN KEY (directive_id) REFERENCES robots_directive_strings (id)
);
CREATE INDEX IF NOT EXISTS idx_robots_url_id ON robots_directives(url_id);
CREATE INDEX IF NOT EXISTS idx_robots_source ON robots_directives(source);
CREATE INDEX IF NOT EXISTS idx_robots_directive_id ON robots_directives(directive_id);

-- Canonical URLs table (references urls table directly)
CREATE TABLE IF NOT EXISTS canonical_urls (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url_id INTEGER NOT NULL,
  canonical_url_id INTEGER NOT NULL,
  source TEXT CHECK (source IN ('html_head', 'http_header')) NOT NULL,
  FOREIGN KEY (url_id) REFERENCES urls (id),
  FOREIGN KEY (canonical_url_id) REFERENCES urls (id)
);
CREATE INDEX IF NOT EXISTS idx_canonical_url_id ON canonical_urls(url_id);
CREATE INDEX IF NOT EXISTS idx_canonical_canonical_url_id ON canonical_urls(canonical_url_id);
CREATE INDEX IF NOT EXISTS idx_canonical_source ON canonical_urls(source);

-- Normalized hreflang language codes table
CREATE TABLE IF NOT EXISTS hreflang_languages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  language_code TEXT UNIQUE NOT NULL  -- e.g., 'en-us', 'fr-ca', 'x-default'
);
CREATE INDEX IF NOT EXISTS idx_hreflang_languages_code ON hreflang_languages(language_code);

-- Hreflang data from XML sitemaps
CREATE TABLE IF NOT EXISTS hreflang_sitemap (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url_id INTEGER NOT NULL,
  hreflang_id INTEGER NOT NULL,
  href_url_id INTEGER NOT NULL,
  FOREIGN KEY (url_id) REFERENCES urls (id),
  FOREIGN KEY (hreflang_id) REFERENCES hreflang_languages (id),
  FOREIGN KEY (href_url_id) REFERENCES urls (id)
);
CREATE INDEX IF NOT EXISTS idx_hreflang_sitemap_url_id ON hreflang_sitemap(url_id);
CREATE INDEX IF NOT EXISTS idx_hreflang_sitemap_lang ON hreflang_sitemap(hreflang_id);

-- Hreflang data from HTTP headers
CREATE TABLE IF NOT EXISTS hreflang_http_header (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url_id INTEGER NOT NULL,
  hreflang_id INTEGER NOT NULL,
  href_url_id INTEGER NOT NULL,
  FOREIGN KEY (url_id) REFERENCES urls (id),
  FOREIGN KEY (hreflang_id) REFERENCES hreflang_languages (id),
  FOREIGN KEY (href_url_id) REFERENCES urls (id)
);
CREATE INDEX IF NOT EXISTS idx_hreflang_http_url_id ON hreflang_http_header(url_id);
CREATE INDEX IF NOT EXISTS idx_hreflang_http_lang ON hreflang_http_header(hreflang_id);

-- Hreflang data from HTML head
CREATE TABLE IF NOT EXISTS hreflang_html_head (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url_id INTEGER NOT NULL,
  hreflang_id INTEGER NOT NULL,
  href_url_id INTEGER NOT NULL,
  FOREIGN KEY (url_id) REFERENCES urls (id),
  FOREIGN KEY (hreflang_id) REFERENCES hreflang_languages (id),
  FOREIGN KEY (href_url_id) REFERENCES urls (id)
);
CREATE INDEX IF NOT EXISTS idx_hreflang_html_url_id ON hreflang_html_head(url_id);
CREATE INDEX IF NOT EXISTS idx_hreflang_html_lang ON hreflang_html_head(hreflang_id);

-- Indexability summary table
CREATE TABLE IF NOT EXISTS indexability (
  url_id INTEGER PRIMARY KEY,
  robots_txt_allows BOOLEAN,
  html_meta_allows BOOLEAN,
  http_header_allows BOOLEAN,
  overall_indexable BOOLEAN,
  robots_txt_directives TEXT,  -- JSON array of robots.txt directives
  html_meta_directives TEXT,   -- JSON array of HTML meta directives
  http_header_directives TEXT, -- JSON array of HTTP header directives
  FOREIGN KEY (url_id) REFERENCES urls (id)
);

-- Sitemap validation table
CREATE TABLE IF NOT EXISTS sitemap_validation (
  url_id INTEGER PRIMARY KEY,
  sitemap_url TEXT NOT NULL,  -- Which sitemap this URL came from
  sitemap_discovered_at INTEGER NOT NULL,  -- When it was discovered from sitemap
  http_status INTEGER,  -- HTTP status when crawled
  is_accessible BOOLEAN,  -- Whether URL returns 200
  is_indexable BOOLEAN,  -- Whether URL is indexable based on robots/canonical
  validation_errors TEXT,  -- JSON array of validation errors
  last_validated INTEGER,  -- When it was last validated
  FOREIGN KEY (url_id) REFERENCES urls (id)
);
CREATE INDEX IF NOT EXISTS idx_indexability_url_id ON indexability(url_id);
CREATE INDEX IF NOT EXISTS idx_indexability_overall ON indexability(overall_indexable);
CREATE INDEX IF NOT EXISTS idx_sitemap_validation_url_id ON sitemap_validation(url_id);
CREATE INDEX IF NOT EXISTS idx_sitemap_validation_sitemap ON sitemap_validation(sitemap_url);
CREATE INDEX IF NOT EXISTS idx_sitemap_validation_accessible ON sitemap_validation(is_accessible);
CREATE INDEX IF NOT EXISTS idx_sitemap_validation_indexable ON sitemap_validation(is_indexable);

-- Redirects table to track redirect chains
CREATE TABLE IF NOT EXISTS redirects (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_url_id INTEGER NOT NULL,  -- Original URL that was requested
  target_url_id INTEGER NOT NULL,  -- Final URL after redirects
  redirect_chain TEXT NOT NULL,    -- JSON array of [{"url": "...", "status": 301, "headers": {...}}, ...]
  chain_length INTEGER NOT NULL,   -- Number of redirects in the chain
  final_status INTEGER NOT NULL,   -- Final HTTP status code
  discovered_at INTEGER NOT NULL,  -- When this redirect was discovered
  FOREIGN KEY (source_url_id) REFERENCES urls (id),
  FOREIGN KEY (target_url_id) REFERENCES urls (id)
);
CREATE INDEX IF NOT EXISTS idx_redirects_source ON redirects(source_url_id);
CREATE INDEX IF NOT EXISTS idx_redirects_target ON redirects(target_url_id);
CREATE INDEX IF NOT EXISTS idx_redirects_chain_length ON redirects(chain_length);

-- Persistent crawl frontier for pause/resume
CREATE TABLE IF NOT EXISTS frontier (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url_id INTEGER NOT NULL,
  depth INTEGER NOT NULL,
  parent_id INTEGER,
  status TEXT NOT NULL CHECK (status IN ('queued','done')),
  enqueued_at INTEGER,
  updated_at INTEGER,
  FOREIGN KEY (url_id) REFERENCES urls (id),
  FOREIGN KEY (parent_id) REFERENCES urls (id),
  UNIQUE(url_id)
);
CREATE INDEX IF NOT EXISTS idx_frontier_status ON frontier(status);
CREATE INDEX IF NOT EXISTS idx_frontier_url_id ON frontier(url_id);

-- Sitemap tracking table
CREATE TABLE IF NOT EXISTS sitemaps_listed (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url_id INTEGER NOT NULL,
  sitemap_url TEXT NOT NULL,  -- Which sitemap this URL was found in
  sitemap_position INTEGER,  -- Position in sitemap (if available)
  discovered_at INTEGER NOT NULL,
  FOREIGN KEY (url_id) REFERENCES urls (id)
);
CREATE INDEX IF NOT EXISTS idx_sitemaps_listed_url_id ON sitemaps_listed(url_id);
CREATE INDEX IF NOT EXISTS idx_sitemaps_listed_sitemap ON sitemaps_listed(sitemap_url);

-- View for comprehensive page analysis
CREATE VIEW IF NOT EXISTS page_analysis AS
SELECT 
    u.url,
    u.kind,
    u.classification,
    c.title,
    c.meta_description,
    c.h1_tags,
    c.h2_tags,
    c.word_count,
    c.html_lang,
    c.internal_links_count,
    c.external_links_count,
    c.internal_links_unique_count,
    c.external_links_unique_count,
    c.crawl_depth,
    c.inlinks_count,
    c.inlinks_unique_count,
    i.robots_txt_allows,
    i.html_meta_allows,
    i.http_header_allows,
    i.overall_indexable,
    i.robots_txt_directives,
    i.html_meta_directives,
    i.http_header_directives,
    GROUP_CONCAT(DISTINCT canonical_urls_table.url) as canonical_urls,
    GROUP_CONCAT(DISTINCT cu.source) as canonical_sources,
    -- Find the hreflang language that points to this page itself (excluding x-default)
    (SELECT hl_self.language_code 
     FROM hreflang_sitemap hs_self 
     JOIN hreflang_languages hl_self ON hs_self.hreflang_id = hl_self.id 
     JOIN urls href_self ON hs_self.href_url_id = href_self.id 
     WHERE hs_self.url_id = u.id 
     AND (href_self.url = u.url OR href_self.url = u.url || '/' OR u.url = href_self.url || '/')
     AND hl_self.language_code != 'x-default' 
     LIMIT 1) as self_hreflang
FROM urls u
LEFT JOIN content c ON u.id = c.url_id
LEFT JOIN indexability i ON u.id = i.url_id
LEFT JOIN canonical_urls cu ON u.id = cu.url_id
LEFT JOIN urls canonical_urls_table ON cu.canonical_url_id = canonical_urls_table.id
WHERE u.classification IN ('internal', 'network')  -- Only show internal and network URLs
GROUP BY u.id;

-- View for internal links with normalized data
CREATE VIEW IF NOT EXISTS internal_links_analysis AS
SELECT 
    u1.url as source_url,
    u2.url as target_url,
    at.text as anchor_text,
    x.xpath,
    h.href,
    h.is_absolute,
    il.url_fragment,
    il.url_parameters,
    il.discovered_at
FROM internal_links il
JOIN urls u1 ON il.source_url_id = u1.id
LEFT JOIN urls u2 ON il.target_url_id = u2.id
LEFT JOIN anchor_texts at ON il.anchor_text_id = at.id
LEFT JOIN xpaths x ON il.xpath_id = x.id
LEFT JOIN hrefs h ON il.href_id = h.id;
"""

async def init_pages_db(db_path: str = PAGES_DB_PATH):
    async with aiosqlite.connect(db_path) as db:
        # Execute each statement separately
        for stmt in PAGES_SCHEMA.split(";\n"):
            if stmt.strip():
                await db.execute(stmt)
        await db.commit()

async def init_crawl_db(db_path: str = CRAWL_DB_PATH):
    async with aiosqlite.connect(db_path) as db:
        # run both URL index + frontier schemas
        for stmt in CRAWL_SCHEMA.split(";\n"):
            if stmt.strip():
                await db.execute(stmt)
        await db.commit()

# ------------------ URL classification ------------------

def classify_url(url: str, base_domain: str, is_from_sitemap: bool = False) -> str:
    """Classify URL as internal, network, external, or social."""
    from urllib.parse import urlparse
    
    parsed = urlparse(url)
    url_domain = parsed.netloc.lower()
    
    # Remove www. prefix for comparison
    if url_domain.startswith('www.'):
        url_domain = url_domain[4:]
    if base_domain.startswith('www.'):
        base_domain = base_domain[4:]
    
    # Social media domains
    social_domains = {
        'facebook.com', 'fb.com', 'twitter.com', 'x.com', 'instagram.com', 
        'linkedin.com', 'youtube.com', 'tiktok.com', 'snapchat.com', 
        'pinterest.com', 'reddit.com', 'discord.com', 'telegram.org',
        'whatsapp.com', 'messenger.com', 'skype.com', 'zoom.us'
    }
    
    # Check if it's a social media domain
    for social_domain in social_domains:
        if url_domain == social_domain or url_domain.endswith('.' + social_domain):
            return 'social'
    
    # Check if it's internal (same domain)
    if url_domain == base_domain:
        return 'internal'
    
    # If it's from a sitemap and not internal, classify as network
    if is_from_sitemap:
        return 'network'
    
    # Everything else is external
    return 'external'

# ------------------ URL ID management ------------------

async def get_or_create_url_id(url: str, base_domain: str, db_path: str = CRAWL_DB_PATH) -> int:
    """Get URL ID, creating the URL record if it doesn't exist."""
    async with aiosqlite.connect(db_path) as db:
        # Try to get existing URL ID
        cursor = await db.execute("SELECT id FROM urls WHERE url = ?", (url,))
        row = await cursor.fetchone()
        if row:
            return row[0]
        
        # Classify the URL
        classification = classify_url(url, base_domain)
        
        # Create new URL record
        cursor = await db.execute(
            "INSERT INTO urls (url, classification, first_seen, last_seen) VALUES (?, ?, ?, ?)",
            (url, classification, int(time.time()), int(time.time()))
        )
        await db.commit()
        return cursor.lastrowid

async def get_url_by_id(url_id: int, db_path: str = CRAWL_DB_PATH) -> str | None:
    """Get URL string by ID."""
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute("SELECT url FROM urls WHERE id = ?", (url_id,))
        row = await cursor.fetchone()
        return row[0] if row else None

# ------------------ writers ------------------

async def write_page(url: str, final_url: str, status: int, headers: dict, html: str, base_domain: str, pages_db_path: str = PAGES_DB_PATH, crawl_db_path: str = CRAWL_DB_PATH):
    now = int(time.time())
    
    # Get URL IDs
    url_id = await get_or_create_url_id(url, base_domain, crawl_db_path)
    final_url_id = await get_or_create_url_id(final_url, base_domain, crawl_db_path) if final_url != url else url_id
    
    async with aiosqlite.connect(pages_db_path) as db:
        await db.execute(
            """
        INSERT INTO pages(url_id, final_url_id, status, fetched_at, headers_json, html_compressed)
        VALUES (?,?,?,?,?,?)
        ON CONFLICT(url_id) DO UPDATE SET
          final_url_id=excluded.final_url_id,
          status=excluded.status,
          fetched_at=excluded.fetched_at,
          headers_json=excluded.headers_json,
          html_compressed=excluded.html_compressed
        """,
            (url_id, final_url_id, status, now, json.dumps(headers, ensure_ascii=False), compress_html(html)),
        )
        await db.commit()

async def upsert_url(url: str, kind: str, base_domain: str, discovered_from: Optional[str] = None, db_path: str = CRAWL_DB_PATH):
    now = int(time.time())
    
    # Get discovered_from_id if provided
    discovered_from_id = None
    if discovered_from:
        discovered_from_id = await get_or_create_url_id(discovered_from, base_domain, db_path)
    
    # Classify the URL
    classification = classify_url(url, base_domain)
    
    async with aiosqlite.connect(db_path) as db:
        await db.execute(
            """
        INSERT INTO urls(url, kind, classification, discovered_from_id, first_seen, last_seen)
        VALUES (?,?,?,?,?,?)
        ON CONFLICT(url) DO UPDATE SET
          kind=excluded.kind,
          classification=excluded.classification,
          discovered_from_id=COALESCE(urls.discovered_from_id, excluded.discovered_from_id),
          last_seen=excluded.last_seen
        """,
            (url, kind, classification, discovered_from_id, now, now),
        )
        await db.commit()

# ------------------ batch writers ------------------

async def batch_write_pages(pages_data: List[Tuple[str, str, int, dict, str, str]], pages_db_path: str = PAGES_DB_PATH, crawl_db_path: str = CRAWL_DB_PATH, batch_size: int = 50):
    """Batch write multiple pages for better performance."""
    if not pages_data:
        return
    
    # Process in smaller batches to avoid timeouts
    for i in range(0, len(pages_data), batch_size):
        batch = pages_data[i:i + batch_size]
        await _batch_write_pages_chunk(batch, pages_db_path, crawl_db_path)

async def _batch_write_pages_chunk(pages_data: List[Tuple[str, str, int, dict, str, str]], pages_db_path: str, crawl_db_path: str):
    """Write a chunk of pages."""
    
    async with aiosqlite.connect(pages_db_path) as pages_conn, aiosqlite.connect(crawl_db_path) as crawl_conn:
        # Prepare batch data
        batch_data = []
        for url, final_url, status, headers, html, base_domain in pages_data:
            # Get URL IDs
            url_id = await get_or_create_url_id_with_conn(url, base_domain, crawl_db_path, crawl_conn)
            final_url_id = await get_or_create_url_id_with_conn(final_url, base_domain, crawl_db_path, crawl_conn) if final_url != url else url_id
            
            batch_data.append((
                url_id, final_url_id, status, int(time.time()),
                json.dumps(headers, ensure_ascii=False), compress_html(html)
            ))
        
        # Batch insert
        await pages_conn.executemany(
            """
            INSERT INTO pages(url_id, final_url_id, status, fetched_at, headers_json, html_compressed)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(url_id) DO UPDATE SET
              final_url_id=excluded.final_url_id,
              status=excluded.status,
              fetched_at=excluded.fetched_at,
              headers_json=excluded.headers_json,
              html_compressed=excluded.html_compressed
            """,
            batch_data
        )
        await pages_conn.commit()

async def batch_upsert_urls(urls_data: List[Tuple], db_path: str = CRAWL_DB_PATH, batch_size: int = 100):
    """Batch upsert multiple URLs for better performance."""
    if not urls_data:
        return
    
    # Process in smaller batches to avoid timeouts
    for i in range(0, len(urls_data), batch_size):
        batch = urls_data[i:i + batch_size]
        await _batch_upsert_urls_chunk(batch, db_path)

async def _batch_upsert_urls_chunk(urls_data: List[Tuple], db_path: str):
    """Upsert a chunk of URLs."""
    
    async with aiosqlite.connect(db_path) as conn:
        # Prepare batch data
        batch_data = []
        now = int(time.time())
        
        for url_data in urls_data:
            # Handle both old (4 params) and new (5 params) formats
            if len(url_data) == 4:
                url, kind, base_domain, discovered_from = url_data
                is_from_sitemap = False
            else:
                url, kind, base_domain, discovered_from, is_from_sitemap = url_data
            # Get discovered_from_id if provided
            discovered_from_id = None
            if discovered_from:
                discovered_from_id = await get_or_create_url_id_with_conn(discovered_from, base_domain, db_path, conn)
            
            # Classify the URL
            classification = classify_url(url, base_domain, is_from_sitemap)
            
            batch_data.append((url, kind, classification, discovered_from_id, now, now))
        
        # Batch insert
        await conn.executemany(
            """
            INSERT INTO urls(url, kind, classification, discovered_from_id, first_seen, last_seen)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(url) DO UPDATE SET
              kind=excluded.kind,
              classification=excluded.classification,
              discovered_from_id=COALESCE(urls.discovered_from_id, excluded.discovered_from_id),
              last_seen=excluded.last_seen
            """,
            batch_data
        )
        await conn.commit()

async def batch_enqueue_frontier(children_data: List[Tuple[str, int, Optional[str], str]], db_path: str = CRAWL_DB_PATH, batch_size: int = 200):
    """Batch enqueue multiple frontier items for better performance."""
    if not children_data:
        return
    
    # Process in smaller batches to avoid timeouts
    for i in range(0, len(children_data), batch_size):
        batch = children_data[i:i + batch_size]
        await _batch_enqueue_frontier_chunk(batch, db_path)

async def _batch_enqueue_frontier_chunk(children_data: List[Tuple[str, int, Optional[str], str]], db_path: str):
    """Enqueue a chunk of frontier items."""
    
    async with aiosqlite.connect(db_path) as conn:
        # Prepare batch data
        batch_data = []
        now = int(time.time())
        
        for url, depth, parent_url, base_domain in children_data:
            url_id = await get_or_create_url_id_with_conn(url, base_domain, db_path, conn)
            parent_id = await get_or_create_url_id_with_conn(parent_url, base_domain, db_path, conn) if parent_url else None
            
            batch_data.append((url_id, depth, parent_id, 'queued', now, now))
        
        # Batch insert
        await conn.executemany(
            """
            INSERT OR IGNORE INTO frontier(url_id, depth, parent_id, status, enqueued_at, updated_at)
            VALUES (?,?,?,?,?,?)
            """,
            batch_data
        )
        await conn.commit()

async def batch_write_content(content_data: List[Tuple[int, str, str, str, str, str, str, int, bool]], db_path: str = CRAWL_DB_PATH, batch_size: int = 50):
    """Batch write content extraction data for better performance."""
    if not content_data:
        return
    
    # Process in smaller batches to avoid timeouts
    for i in range(0, len(content_data), batch_size):
        batch = content_data[i:i + batch_size]
        await _batch_write_content_chunk(batch, db_path)

async def _batch_write_content_chunk(content_data: List[Tuple[int, str, str, str, str, str, str, int, bool]], db_path: str):
    """Write a chunk of content data."""
    
    async with aiosqlite.connect(db_path) as conn:
        # Batch insert
        await conn.executemany(
            """
            INSERT INTO content(url_id, title, meta_description, meta_robots, canonical_url, h1_tags, h2_tags, word_count, is_indexable)
            VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(url_id) DO UPDATE SET
              title=excluded.title,
              meta_description=excluded.meta_description,
              meta_robots=excluded.meta_robots,
              canonical_url=excluded.canonical_url,
              h1_tags=excluded.h1_tags,
              h2_tags=excluded.h2_tags,
              word_count=excluded.word_count,
              is_indexable=excluded.is_indexable
            """,
            content_data
        )
        await conn.commit()

async def batch_write_content_with_url_resolution(content_data: List[Tuple[str, dict, str]], crawl_db_path: str):
    """Write content data with URL ID resolution and normalized tables."""
    if not content_data:
        return
    
    async with aiosqlite.connect(crawl_db_path) as conn:
        for url, content_info, base_domain in content_data:
            # Get URL ID
            cursor = await conn.execute("SELECT id FROM urls WHERE url = ?", (url,))
            row = await cursor.fetchone()
            if not row:
                continue
            
            url_id = row[0]
            
            # Insert/update content
            await conn.execute(
                """
                INSERT INTO content(url_id, title, meta_description, h1_tags, h2_tags, word_count, html_lang)
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(url_id) DO UPDATE SET
                  title=excluded.title,
                  meta_description=excluded.meta_description,
                  h1_tags=excluded.h1_tags,
                  h2_tags=excluded.h2_tags,
                  word_count=excluded.word_count,
                  html_lang=excluded.html_lang
                """,
                (
                    url_id,
                    content_info['title'],
                    content_info['meta_description'],
                    json.dumps(content_info['h1_tags'], ensure_ascii=False),
                    json.dumps(content_info['h2_tags'], ensure_ascii=False),
                    content_info['word_count'],
                    content_info['html_lang']
                )
            )
            
            # Insert robots directives from HTML meta
            if content_info['html_meta_directives']:
                for directive in content_info['html_meta_directives']:
                    directive_id = await get_or_create_robots_directive_id(directive, conn)
                    await conn.execute(
                        """
                        INSERT OR IGNORE INTO robots_directives(url_id, source, directive_id)
                        VALUES (?, 'html_meta', ?)
                        """,
                        (url_id, directive_id)
                    )
            
            # Insert robots directives from HTTP headers
            if content_info['http_header_directives']:
                for directive in content_info['http_header_directives']:
                    directive_id = await get_or_create_robots_directive_id(directive, conn)
                    await conn.execute(
                        """
                        INSERT OR IGNORE INTO robots_directives(url_id, source, directive_id)
                        VALUES (?, 'http_header', ?)
                        """,
                        (url_id, directive_id)
                    )
            
            # Insert canonical URL from HTML head
            if content_info['canonical_url']:
                canonical_url_id = await get_or_create_canonical_url_id(content_info['canonical_url'], base_domain, conn)
                await conn.execute(
                    """
                    INSERT OR IGNORE INTO canonical_urls(url_id, canonical_url_id, source)
                    VALUES (?, ?, 'html_head')
                    """,
                    (url_id, canonical_url_id)
                )
            
            # Calculate indexability
            html_meta_allows = not any('noindex' in d for d in content_info['html_meta_directives'])
            http_header_allows = not any('noindex' in d for d in content_info['http_header_directives'])
            
            # Check robots.txt for this URL
            from urllib.parse import urlparse
            from .robots import is_url_crawlable
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            robots_txt_allows = is_url_crawlable(url, "SQLiteCrawler/0.2")
            
            # Store robots.txt directives if any
            robots_txt_directives = []
            if not robots_txt_allows:
                robots_txt_directives.append('disallow')
            
            # Insert/update indexability summary
            await conn.execute(
                """
                INSERT INTO indexability(url_id, robots_txt_allows, html_meta_allows, http_header_allows, 
                                       robots_txt_directives, html_meta_directives, http_header_directives, overall_indexable)
                VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT(url_id) DO UPDATE SET
                  robots_txt_allows=excluded.robots_txt_allows,
                  html_meta_allows=excluded.html_meta_allows,
                  http_header_allows=excluded.http_header_allows,
                  robots_txt_directives=excluded.robots_txt_directives,
                  html_meta_directives=excluded.html_meta_directives,
                  http_header_directives=excluded.http_header_directives,
                  overall_indexable=excluded.overall_indexable
                """,
                (
                    url_id,
                    robots_txt_allows,
                    html_meta_allows,
                    http_header_allows,
                    json.dumps(robots_txt_directives, ensure_ascii=False),
                    json.dumps(content_info['html_meta_directives'], ensure_ascii=False),
                    json.dumps(content_info['http_header_directives'], ensure_ascii=False),
                    robots_txt_allows and html_meta_allows and http_header_allows  # Overall indexable
                )
            )
        
        await conn.commit()

async def batch_write_internal_links(links_data: List[Tuple[str, list, str]], crawl_db_path: str):
    """Write internal links data with normalized references and URL components."""
    if not links_data:
        return
    
    async with aiosqlite.connect(crawl_db_path) as conn:
        for source_url, detailed_links, base_domain in links_data:
            # Get source URL ID
            cursor = await conn.execute("SELECT id FROM urls WHERE url = ?", (source_url,))
            row = await cursor.fetchone()
            if not row:
                continue
            
            source_url_id = row[0]
            now = int(time.time())
            
            # Count internal vs external links
            internal_count = 0
            external_count = 0
            internal_unique = set()
            external_unique = set()
            
            for link_info in detailed_links:
                target_url = link_info['url']
                href_original = link_info['href']
                
                # Parse URL components
                url_components = parse_url_components(href_original, source_url)
                
                # Get or create normalized IDs
                anchor_text_id = await get_or_create_anchor_text_id(link_info['anchor_text'], conn)
                xpath_id = await get_or_create_xpath_id(link_info['xpath'], conn)
                href_id = await get_or_create_href_id(url_components['href'], url_components['is_absolute'], conn)
                
                # Try to get target URL ID (may not exist yet)
                target_url_id = None
                if url_components['href']:
                    cursor = await conn.execute("SELECT id FROM urls WHERE url = ?", (url_components['href'],))
                    row = await cursor.fetchone()
                    if row:
                        target_url_id = row[0]
                
                # Classify the link
                classification = classify_url(target_url, base_domain)
                
                if classification == 'internal':
                    internal_count += 1
                    internal_unique.add(url_components['href'])
                    
                    # Insert internal link with fully normalized references
                    await conn.execute(
                        """
                        INSERT OR IGNORE INTO internal_links(
                            source_url_id, target_url_id, anchor_text_id, xpath_id, href_id,
                            url_fragment, url_parameters, discovered_at
                        )
                        VALUES (?,?,?,?,?,?,?,?)
                        """,
                        (
                            source_url_id,
                            target_url_id,
                            anchor_text_id,
                            xpath_id,
                            href_id,
                            url_components['url_fragment'],
                            url_components['url_parameters'],
                            now
                        )
                    )
                else:
                    external_count += 1
                    external_unique.add(url_components['href'])
            
            # Update content table with link counts
            await conn.execute(
                """
                UPDATE content 
                SET internal_links_count = ?, 
                    external_links_count = ?,
                    internal_links_unique_count = ?,
                    external_links_unique_count = ?
                WHERE url_id = ?
                """,
                (
                    internal_count,
                    external_count,
                    len(internal_unique),
                    len(external_unique),
                    source_url_id
                )
            )
        
        await conn.commit()

async def get_or_create_anchor_text_id(anchor_text: str, conn: aiosqlite.Connection) -> int:
    """Get or create anchor text ID."""
    cursor = await conn.execute("SELECT id FROM anchor_texts WHERE text = ?", (anchor_text,))
    row = await cursor.fetchone()
    if row:
        return row[0]
    
    cursor = await conn.execute("INSERT INTO anchor_texts (text) VALUES (?)", (anchor_text,))
    return cursor.lastrowid

async def get_or_create_xpath_id(xpath: str, conn: aiosqlite.Connection) -> int:
    """Get or create xpath ID."""
    cursor = await conn.execute("SELECT id FROM xpaths WHERE xpath = ?", (xpath,))
    row = await cursor.fetchone()
    if row:
        return row[0]
    
    cursor = await conn.execute("INSERT INTO xpaths (xpath) VALUES (?)", (xpath,))
    return cursor.lastrowid

async def get_or_create_href_id(href: str, is_absolute: bool, conn: aiosqlite.Connection) -> int:
    """Get or create href ID."""
    cursor = await conn.execute("SELECT id FROM hrefs WHERE href = ?", (href,))
    row = await cursor.fetchone()
    if row:
        return row[0]
    
    cursor = await conn.execute("INSERT INTO hrefs (href, is_absolute) VALUES (?, ?)", (href, is_absolute))
    return cursor.lastrowid

async def get_or_create_canonical_url_id(canonical_url: str, base_domain: str, conn: aiosqlite.Connection) -> int:
    """Get or create canonical URL ID in the urls table."""
    import time
    
    # First try to get existing URL ID
    cursor = await conn.execute("SELECT id FROM urls WHERE url = ?", (canonical_url,))
    row = await cursor.fetchone()
    if row:
        return row[0]
    
    # If not found, create new URL entry
    classification = classify_url(canonical_url, base_domain)
    now = int(time.time())
    cursor = await conn.execute(
        "INSERT INTO urls (url, classification, first_seen, last_seen) VALUES (?, ?, ?, ?)",
        (canonical_url, classification, now, now)
    )
    return cursor.lastrowid

async def get_or_create_robots_directive_id(directive: str, conn: aiosqlite.Connection) -> int:
    """Get or create robots directive ID."""
    cursor = await conn.execute("SELECT id FROM robots_directive_strings WHERE directive = ?", (directive,))
    row = await cursor.fetchone()
    if row:
        return row[0]
    
    cursor = await conn.execute("INSERT INTO robots_directive_strings (directive) VALUES (?)", (directive,))
    return cursor.lastrowid

def parse_url_components(href: str, base_url: str) -> dict:
    """Parse URL into components: href (without fragment/params), fragment, parameters."""
    from urllib.parse import urlparse, urljoin, parse_qs, urlunparse
    
    # Parse the original href
    parsed_href = urlparse(href)
    is_absolute = bool(parsed_href.netloc)
    
    # Create href without fragment and parameters
    clean_href = urlunparse((parsed_href.scheme, parsed_href.netloc, parsed_href.path, 
                           parsed_href.params, '', ''))
    
    # If relative, try to resolve to absolute
    if not is_absolute:
        try:
            resolved = urljoin(base_url, clean_href)
            clean_href = resolved
        except:
            pass  # Keep original if resolution fails
    
    # Extract fragment and parameters (only if present)
    url_fragment = parsed_href.fragment if parsed_href.fragment else None
    url_parameters = None
    if parsed_href.query:
        # Convert query string to a more readable format
        params = parse_qs(parsed_href.query)
        url_parameters = "&".join([f"{k}={v[0]}" for k, v in params.items()])
    
    return {
        'href': clean_href,
        'url_fragment': url_fragment,
        'url_parameters': url_parameters,
        'is_absolute': is_absolute
    }

async def get_or_create_hreflang_language_id(language_code: str, conn: aiosqlite.Connection) -> int:
    """Get or create hreflang language ID."""
    cursor = await conn.execute("SELECT id FROM hreflang_languages WHERE language_code = ?", (language_code,))
    row = await cursor.fetchone()
    if row:
        return row[0]
    
    # Create new language code
    cursor = await conn.execute("INSERT INTO hreflang_languages(language_code) VALUES (?)", (language_code,))
    return cursor.lastrowid

async def batch_write_hreflang_sitemap_data(hreflang_data: List[Tuple[str, str, str]], crawl_db_path: str):
    """Write hreflang data from sitemaps to the normalized database structure."""
    if not hreflang_data:
        return
    
    async with aiosqlite.connect(crawl_db_path) as conn:
        for url, hreflang, href_url in hreflang_data:
            # Get source URL ID
            cursor = await conn.execute("SELECT id FROM urls WHERE url = ?", (url,))
            source_row = await cursor.fetchone()
            if not source_row:
                continue
            
            source_url_id = source_row[0]
            
            # Get target URL ID (create if doesn't exist)
            cursor = await conn.execute("SELECT id FROM urls WHERE url = ?", (href_url,))
            target_row = await cursor.fetchone()
            if not target_row:
                # Create the target URL if it doesn't exist
                from urllib.parse import urlparse
                parsed = urlparse(href_url)
                base_domain = parsed.netloc
                # Classify as network since it's from sitemap hreflang data
                classification = classify_url(href_url, base_domain, is_from_sitemap=True)
                await conn.execute(
                    """
                    INSERT INTO urls(url, kind, classification, first_seen, last_seen)
                    VALUES (?, 'other', ?, ?, ?)
                    """,
                    (href_url, classification, int(__import__('time').time()), int(__import__('time').time()))
                )
                await conn.commit()
                
                # Get the newly created URL ID
                cursor = await conn.execute("SELECT id FROM urls WHERE url = ?", (href_url,))
                target_row = await cursor.fetchone()
                if not target_row:
                    continue
            
            target_url_id = target_row[0]
            
            # Get or create hreflang language ID
            hreflang_id = await get_or_create_hreflang_language_id(hreflang, conn)
            
            # Insert hreflang sitemap data
            await conn.execute(
                """
                INSERT OR IGNORE INTO hreflang_sitemap(url_id, hreflang_id, href_url_id)
                VALUES (?,?,?)
                """,
                (source_url_id, hreflang_id, target_url_id)
            )
        
        await conn.commit()

async def batch_write_sitemap_validation(sitemap_urls: List[Tuple[str, str]], crawl_db_path: str):
    """Write sitemap validation records for discovered URLs."""
    if not sitemap_urls:
        return
    
    async with aiosqlite.connect(crawl_db_path) as conn:
        now = int(time.time())
        for url, sitemap_url in sitemap_urls:
            # Get URL ID
            cursor = await conn.execute("SELECT id FROM urls WHERE url = ?", (url,))
            row = await cursor.fetchone()
            if not row:
                continue
            
            url_id = row[0]
            
            # Insert sitemap validation record
            await conn.execute(
                """
                INSERT OR IGNORE INTO sitemap_validation(url_id, sitemap_url, sitemap_discovered_at)
                VALUES (?,?,?)
                """,
                (url_id, sitemap_url, now)
            )
        
        await conn.commit()

async def batch_write_redirects(redirect_data: List[Tuple[str, str, str, int, int]], crawl_db_path: str):
    """Write redirect chain data to the database."""
    if not redirect_data:
        return
    
    async with aiosqlite.connect(crawl_db_path) as conn:
        now = int(time.time())
        for source_url, target_url, redirect_chain_json, chain_length, final_status in redirect_data:
            # Get source URL ID
            cursor = await conn.execute("SELECT id FROM urls WHERE url = ?", (source_url,))
            source_row = await cursor.fetchone()
            if not source_row:
                continue
            
            source_url_id = source_row[0]
            
            # Get target URL ID (create if doesn't exist)
            cursor = await conn.execute("SELECT id FROM urls WHERE url = ?", (target_url,))
            target_row = await cursor.fetchone()
            if not target_row:
                # Create the target URL if it doesn't exist
                from urllib.parse import urlparse
                parsed = urlparse(target_url)
                base_domain = parsed.netloc
                classification = classify_url(target_url, base_domain)
                await conn.execute(
                    """
                    INSERT INTO urls(url, kind, classification, first_seen, last_seen)
                    VALUES (?, 'other', ?, ?, ?)
                    """,
                    (target_url, classification, now, now)
                )
                await conn.commit()
                
                # Get the newly created URL ID
                cursor = await conn.execute("SELECT id FROM urls WHERE url = ?", (target_url,))
                target_row = await cursor.fetchone()
                if not target_row:
                    continue
            
            target_url_id = target_row[0]
            
            # Insert redirect record
            await conn.execute(
                """
                INSERT OR REPLACE INTO redirects(source_url_id, target_url_id, redirect_chain, chain_length, final_status, discovered_at)
                VALUES (?,?,?,?,?,?)
                """,
                (source_url_id, target_url_id, redirect_chain_json, chain_length, final_status, now)
            )
        
        await conn.commit()

# Helper function for get_or_create_url_id with connection
async def get_or_create_url_id_with_conn(url: str, base_domain: str, db_path: str, conn: aiosqlite.Connection) -> int:
    """Get URL ID, creating the URL record if it doesn't exist (with existing connection)."""
    # Try to get existing URL ID
    cursor = await conn.execute("SELECT id FROM urls WHERE url = ?", (url,))
    row = await cursor.fetchone()
    if row:
        return row[0]
    
    # Classify the URL
    classification = classify_url(url, base_domain)
    
    # Create new URL record
    cursor = await conn.execute(
        "INSERT INTO urls (url, classification, first_seen, last_seen) VALUES (?, ?, ?, ?)",
        (url, classification, int(time.time()), int(time.time()))
    )
    return cursor.lastrowid

# ------------------ frontier (pause/resume) ------------------

async def frontier_seed(start: str, base_domain: str, reset: bool = False, db_path: str = CRAWL_DB_PATH):
    now = int(time.time())
    
    # Get URL ID for start URL
    start_url_id = await get_or_create_url_id(start, base_domain, db_path)
    
    async with aiosqlite.connect(db_path) as db:
        if reset:
            await db.execute("DELETE FROM frontier")
            # After reset, always add the start URL
            await db.execute(
                "INSERT OR IGNORE INTO frontier(url_id, depth, parent_id, status, enqueued_at, updated_at) VALUES (?,?,?,?,?,?)",
                (start_url_id, 0, None, 'queued', now, now),
            )
        else:
            # For non-reset calls (like sitemap URLs), always try to add
            await db.execute(
                "INSERT OR IGNORE INTO frontier(url_id, depth, parent_id, status, enqueued_at, updated_at) VALUES (?,?,?,?,?,?)",
                (start_url_id, 0, None, 'queued', now, now),
            )
        await db.commit()

async def frontier_next_batch(limit: int, db_path: str = CRAWL_DB_PATH) -> List[Tuple[str, int, Optional[str]]]:
    async with aiosqlite.connect(db_path) as db:
        cur = await db.execute(
            """
            SELECT f.url_id, f.depth, f.parent_id, u.url, p.url as parent_url
            FROM frontier f
            JOIN urls u ON f.url_id = u.id
            LEFT JOIN urls p ON f.parent_id = p.id
            WHERE f.status='queued' 
            ORDER BY f.enqueued_at 
            LIMIT ?
            """,
            (limit,),
        )
        rows = await cur.fetchall()
        return [(r[3], r[1], r[4]) for r in rows]  # (url, depth, parent_url)

async def frontier_mark_done(urls: Iterable[str], base_domain: str, db_path: str = CRAWL_DB_PATH):
    now = int(time.time())
    
    # Get URL IDs for the URLs to mark done
    url_ids = []
    for url in urls:
        url_id = await get_or_create_url_id(url, base_domain, db_path)
        url_ids.append(url_id)
    
    async with aiosqlite.connect(db_path) as db:
        await db.executemany(
            "UPDATE frontier SET status='done', updated_at=? WHERE url_id=?",
            [(now, url_id) for url_id in url_ids],
        )
        await db.commit()

async def frontier_enqueue_many(children: Iterable[Tuple[str, int, Optional[str]]], base_domain: str, db_path: str = CRAWL_DB_PATH):
    now = int(time.time())
    
    # Convert URLs to IDs
    children_with_ids = []
    for (url, depth, parent_url) in children:
        url_id = await get_or_create_url_id(url, base_domain, db_path)
        parent_id = await get_or_create_url_id(parent_url, base_domain, db_path) if parent_url else None
        children_with_ids.append((url_id, depth, parent_id))
    
    async with aiosqlite.connect(db_path) as db:
        await db.executemany(
            """
        INSERT OR IGNORE INTO frontier(url_id, depth, parent_id, status, enqueued_at, updated_at)
        VALUES (?,?,?,?,?,?)
        """,
            [(url_id, d, p_id, 'queued', now, now) for (url_id, d, p_id) in children_with_ids],
        )
        await db.commit()

async def frontier_stats(db_path: str = CRAWL_DB_PATH) -> Tuple[int, int]:
    """Return (#queued, #done)."""
    async with aiosqlite.connect(db_path) as db:
        cur = await db.execute("SELECT SUM(status='queued'), SUM(status='done') FROM frontier")
        row = await cur.fetchone()
        return (int(row[0] or 0), int(row[1] or 0))