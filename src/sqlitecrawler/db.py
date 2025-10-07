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

def extract_content_from_html(html: str, headers: dict = None, base_url: str = None) -> dict:
    """Extract title, meta description, robots, canonical, h1, h2 tags, word count, and schema data from HTML."""
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
        
        # Extract hreflang URLs from HTML head
        hreflang_urls = []
        hreflang_links = soup.find_all('link', attrs={'rel': 'alternate', 'hreflang': True})
        for link in hreflang_links:
            href = link.get('href', '').strip()
            hreflang = link.get('hreflang', '').strip()
            if href and hreflang:
                hreflang_urls.append({'url': href, 'hreflang': hreflang})
        
        # Extract HTML lang declaration (check both html and head tags)
        html_tag = soup.find('html')
        html_lang = None
        
        if html_tag:
            html_lang = html_tag.get('lang', '').strip()
        
        # If no lang on html tag, check head tag
        if not html_lang:
            head_tag = soup.find('head')
            if head_tag:
                html_lang = head_tag.get('lang', '').strip()
        
        # If still no lang, check for xml:lang attribute
        if not html_lang and html_tag:
            html_lang = html_tag.get('xml:lang', '').strip()
        
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
        
        # Extract schema data if base_url is provided
        schema_data = []
        if base_url:
            try:
                from .schema import extract_schema_data
                schema_data = extract_schema_data(html, base_url)
            except Exception as e:
                print(f"Error extracting schema data: {e}")
                schema_data = []
        
        return {
            'title': title,
            'meta_description': meta_description,
            'h1_tags': h1_tags,
            'h2_tags': h2_tags,
            'word_count': word_count,
            'html_meta_directives': html_meta_directives,
            'http_header_directives': http_header_directives,
            'canonical_url': canonical_url,
            'html_lang': html_lang,
            'hreflang_urls': hreflang_urls,
            'schema_data': schema_data
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
            'html_lang': None,
            'hreflang_urls': [],
            'schema_data': []
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
  headers_json TEXT,
  html_compressed BLOB,
  FOREIGN KEY (url_id) REFERENCES urls (id),
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
  meta_description_id INTEGER,  -- Reference to normalized meta description
  h1_tags TEXT,  -- JSON array of h1 texts
  h2_tags TEXT,  -- JSON array of h2 texts
  word_count INTEGER,
  html_lang_id INTEGER,  -- Reference to normalized HTML language
  internal_links_count INTEGER DEFAULT 0,
  external_links_count INTEGER DEFAULT 0,
  internal_links_unique_count INTEGER DEFAULT 0,
  external_links_unique_count INTEGER DEFAULT 0,
  crawl_depth INTEGER DEFAULT 0,
  inlinks_count INTEGER DEFAULT 0,
  inlinks_unique_count INTEGER DEFAULT 0,
  FOREIGN KEY (url_id) REFERENCES urls (id),
  FOREIGN KEY (meta_description_id) REFERENCES meta_descriptions (id),
  FOREIGN KEY (html_lang_id) REFERENCES html_languages (id)
);
CREATE INDEX IF NOT EXISTS idx_content_url_id ON content(url_id);

-- Normalized anchor text table
CREATE TABLE IF NOT EXISTS anchor_texts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  text TEXT UNIQUE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_anchor_texts_text ON anchor_texts(text);

-- Normalized HTML language codes table
CREATE TABLE IF NOT EXISTS html_languages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  language_code TEXT UNIQUE NOT NULL  -- e.g., 'en', 'en-US', 'fr-CA'
);
CREATE INDEX IF NOT EXISTS idx_html_languages_code ON html_languages(language_code);

-- Normalized meta descriptions table
CREATE TABLE IF NOT EXISTS meta_descriptions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  description TEXT UNIQUE NOT NULL  -- The actual meta description text
);
CREATE INDEX IF NOT EXISTS idx_meta_descriptions_text ON meta_descriptions(description);

-- Normalized xpath table  
CREATE TABLE IF NOT EXISTS xpaths (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  xpath TEXT UNIQUE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_xpaths_xpath ON xpaths(xpath);

-- Internal links table with direct URL references
CREATE TABLE IF NOT EXISTS internal_links (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_url_id INTEGER NOT NULL,
  target_url_id INTEGER,  -- NULL if target doesn't exist in our DB yet
  anchor_text_id INTEGER,  -- Reference to normalized anchor text
  xpath_id INTEGER,  -- Reference to normalized xpath
  href_url_id INTEGER NOT NULL,  -- The href URL (from urls table)
  url_fragment TEXT,  -- #fragment part (only if present)
  url_parameters TEXT,  -- ?param=value part (only if present)
  discovered_at INTEGER NOT NULL,
  FOREIGN KEY (source_url_id) REFERENCES urls (id),
  FOREIGN KEY (target_url_id) REFERENCES urls (id),
  FOREIGN KEY (anchor_text_id) REFERENCES anchor_texts (id),
  FOREIGN KEY (xpath_id) REFERENCES xpaths (id),
  FOREIGN KEY (href_url_id) REFERENCES urls (id),
  UNIQUE(source_url_id, xpath_id)  -- Prevent duplicate links with same xpath
);
CREATE INDEX IF NOT EXISTS idx_internal_links_source ON internal_links(source_url_id);
CREATE INDEX IF NOT EXISTS idx_internal_links_target ON internal_links(target_url_id);
CREATE INDEX IF NOT EXISTS idx_internal_links_anchor ON internal_links(anchor_text_id);
CREATE INDEX IF NOT EXISTS idx_internal_links_xpath ON internal_links(xpath_id);
CREATE INDEX IF NOT EXISTS idx_internal_links_href ON internal_links(href_url_id);

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

-- Page metadata table (status codes, fetch info, etc.)
CREATE TABLE IF NOT EXISTS page_metadata (
  url_id INTEGER PRIMARY KEY,
  initial_status_code INTEGER,  -- The first HTTP status code received
  final_status_code INTEGER,    -- The final HTTP status code after redirects
  final_url_id INTEGER,         -- The final URL after redirects
  redirect_destination_url_id INTEGER,  -- The immediate redirect destination (for 301/302/etc)
  fetched_at INTEGER,
  etag TEXT,
  last_modified TEXT,
  FOREIGN KEY (url_id) REFERENCES urls (id),
  FOREIGN KEY (final_url_id) REFERENCES urls (id),
  FOREIGN KEY (redirect_destination_url_id) REFERENCES urls (id)
);

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

CREATE INDEX IF NOT EXISTS idx_page_metadata_url_id ON page_metadata(url_id);
CREATE INDEX IF NOT EXISTS idx_page_metadata_initial_status ON page_metadata(initial_status_code);
CREATE INDEX IF NOT EXISTS idx_page_metadata_final_status ON page_metadata(final_status_code);
CREATE INDEX IF NOT EXISTS idx_page_metadata_redirect_dest ON page_metadata(redirect_destination_url_id);

CREATE INDEX IF NOT EXISTS idx_indexability_url_id ON indexability(url_id);
CREATE INDEX IF NOT EXISTS idx_indexability_overall ON indexability(overall_indexable);

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
  priority_score REAL DEFAULT 0.0,
  sitemap_priority REAL DEFAULT 0.5,
  inlinks_count INTEGER DEFAULT 0,
  content_type_score REAL DEFAULT 1.0,
  FOREIGN KEY (url_id) REFERENCES urls (id),
  FOREIGN KEY (parent_id) REFERENCES urls (id),
  UNIQUE(url_id)
);
CREATE INDEX IF NOT EXISTS idx_frontier_status ON frontier(status);
CREATE INDEX IF NOT EXISTS idx_frontier_url_id ON frontier(url_id);
CREATE INDEX IF NOT EXISTS idx_frontier_priority ON frontier(priority_score DESC, enqueued_at ASC);

-- Sitemaps table - tracks discovered sitemap files
CREATE TABLE IF NOT EXISTS sitemaps (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sitemap_url TEXT UNIQUE NOT NULL,
  discovered_at INTEGER NOT NULL,
  last_crawled_at INTEGER,
  total_urls_found INTEGER DEFAULT 0,
  status TEXT DEFAULT 'active'  -- 'active', 'error', 'not_found'
);
CREATE INDEX IF NOT EXISTS idx_sitemaps_url ON sitemaps(sitemap_url);
CREATE INDEX IF NOT EXISTS idx_sitemaps_discovered ON sitemaps(discovered_at);

-- Junction table for URLs found in sitemaps (many-to-many relationship)
CREATE TABLE IF NOT EXISTS url_sitemaps (
  url_id INTEGER NOT NULL,
  sitemap_id INTEGER NOT NULL,
  position INTEGER,  -- Position in sitemap (if available)
  discovered_at INTEGER NOT NULL,
  PRIMARY KEY (url_id, sitemap_id),
  FOREIGN KEY (url_id) REFERENCES urls (id),
  FOREIGN KEY (sitemap_id) REFERENCES sitemaps (id)
);
CREATE INDEX IF NOT EXISTS idx_url_sitemaps_url_id ON url_sitemaps(url_id);
CREATE INDEX IF NOT EXISTS idx_url_sitemaps_sitemap_id ON url_sitemaps(sitemap_id);
CREATE INDEX IF NOT EXISTS idx_url_sitemaps_discovered ON url_sitemaps(discovered_at);

-- Failed URLs retry tracking table
CREATE TABLE IF NOT EXISTS failed_urls (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url_id INTEGER NOT NULL,
  status_code INTEGER NOT NULL,  -- The HTTP status code that caused the failure
  failure_reason TEXT,  -- Additional failure details (timeout, connection error, etc.)
  retry_count INTEGER DEFAULT 0,  -- Number of retry attempts made
  last_retry_at INTEGER,  -- Timestamp of last retry attempt
  next_retry_at INTEGER,  -- When this URL should be retried next
  created_at INTEGER NOT NULL,  -- When this failure was first recorded
  FOREIGN KEY (url_id) REFERENCES urls (id)
);
CREATE INDEX IF NOT EXISTS idx_failed_urls_url_id ON failed_urls(url_id);
CREATE INDEX IF NOT EXISTS idx_failed_urls_status ON failed_urls(status_code);
CREATE INDEX IF NOT EXISTS idx_failed_urls_next_retry ON failed_urls(next_retry_at);
CREATE INDEX IF NOT EXISTS idx_failed_urls_retry_count ON failed_urls(retry_count);

-- Schema.org structured data tables
-- Normalized schema types table
CREATE TABLE IF NOT EXISTS schema_types (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  type_name TEXT UNIQUE NOT NULL  -- e.g., 'Article', 'Product', 'Organization', 'BreadcrumbList'
);
CREATE INDEX IF NOT EXISTS idx_schema_types_name ON schema_types(type_name);

-- Schema.org structured data instances (normalized by content hash)
CREATE TABLE IF NOT EXISTS schema_instances (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  content_hash TEXT UNIQUE NOT NULL,  -- SHA256 hash of normalized content
  schema_type_id INTEGER NOT NULL,
  format TEXT CHECK (format IN ('json-ld', 'microdata', 'rdfa')) NOT NULL,
  raw_data TEXT NOT NULL,  -- The original structured data
  parsed_data TEXT,  -- Normalized/parsed data as JSON
  is_valid BOOLEAN DEFAULT TRUE,  -- Whether the structured data is valid
  validation_errors TEXT,  -- JSON array of validation errors if any
  severity TEXT DEFAULT 'info' CHECK (severity IN ('info', 'warning', 'error', 'critical')),  -- Severity of validation issues
  created_at INTEGER NOT NULL,
  FOREIGN KEY (schema_type_id) REFERENCES schema_types (id)
);

-- Page schema references (links pages to schema instances)
CREATE TABLE IF NOT EXISTS page_schema_references (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url_id INTEGER NOT NULL,
  schema_instance_id INTEGER NOT NULL,
  position INTEGER,  -- Position on page (for multiple instances of same type)
  property_name TEXT,  -- For nested properties (e.g., 'image', 'breadcrumb', 'video')
  is_main_entity BOOLEAN DEFAULT FALSE,  -- Whether this is the main entity for the page
  parent_entity_id INTEGER,  -- Reference to parent entity if this is a nested property
  discovered_at INTEGER NOT NULL,
  FOREIGN KEY (url_id) REFERENCES urls (id),
  FOREIGN KEY (schema_instance_id) REFERENCES schema_instances (id),
  FOREIGN KEY (parent_entity_id) REFERENCES page_schema_references (id)
);

-- Legacy schema_data table (for migration compatibility)
CREATE TABLE IF NOT EXISTS schema_data (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url_id INTEGER NOT NULL,
  schema_type_id INTEGER NOT NULL,
  format TEXT CHECK (format IN ('json-ld', 'microdata', 'rdfa')) NOT NULL,
  raw_data TEXT NOT NULL,  -- The original structured data (JSON for JSON-LD, HTML for microdata/rdfa)
  parsed_data TEXT,  -- Normalized/parsed data as JSON
  position INTEGER,  -- Position on page (for multiple instances of same type)
  is_valid BOOLEAN DEFAULT TRUE,  -- Whether the structured data is valid
  validation_errors TEXT,  -- JSON array of validation errors if any
  severity TEXT DEFAULT 'info' CHECK (severity IN ('info', 'warning', 'error', 'critical')),  -- Severity of validation issues
  discovered_at INTEGER NOT NULL,
  FOREIGN KEY (url_id) REFERENCES urls (id),
  FOREIGN KEY (schema_type_id) REFERENCES schema_types (id)
);
-- Indexes for schema_instances table
CREATE INDEX IF NOT EXISTS idx_schema_instances_hash ON schema_instances(content_hash);
CREATE INDEX IF NOT EXISTS idx_schema_instances_type ON schema_instances(schema_type_id);
CREATE INDEX IF NOT EXISTS idx_schema_instances_format ON schema_instances(format);
CREATE INDEX IF NOT EXISTS idx_schema_instances_valid ON schema_instances(is_valid);

-- Indexes for page_schema_references table
CREATE INDEX IF NOT EXISTS idx_page_schema_refs_url_id ON page_schema_references(url_id);
CREATE INDEX IF NOT EXISTS idx_page_schema_refs_instance_id ON page_schema_references(schema_instance_id);
CREATE INDEX IF NOT EXISTS idx_page_schema_refs_main_entity ON page_schema_references(is_main_entity);
CREATE INDEX IF NOT EXISTS idx_page_schema_refs_parent ON page_schema_references(parent_entity_id);

-- Legacy indexes for schema_data table
CREATE INDEX IF NOT EXISTS idx_schema_data_url_id ON schema_data(url_id);
CREATE INDEX IF NOT EXISTS idx_schema_data_type ON schema_data(schema_type_id);
CREATE INDEX IF NOT EXISTS idx_schema_data_format ON schema_data(format);
CREATE INDEX IF NOT EXISTS idx_schema_data_valid ON schema_data(is_valid);

-- View for comprehensive page analysis
CREATE VIEW IF NOT EXISTS view_crawl_overview AS
SELECT 
    u.url,
    COALESCE(f.status, 'unknown') as crawl_status,
    pm.initial_status_code as status_code,
    i.overall_indexable as indexable,
    -- Add indexability reason when not indexable
    CASE 
        WHEN i.overall_indexable = 0 THEN
            CASE 
                WHEN i.robots_txt_allows = 0 THEN 'blocked by robots.txt'
                WHEN i.html_meta_allows = 0 THEN 'blocked by meta robots'
                WHEN i.http_header_allows = 0 THEN 'blocked by HTTP headers'
                WHEN pm.initial_status_code != 200 THEN 'not 200 status'
                WHEN canonical_urls_table.url IS NOT NULL AND canonical_urls_table.url != u.url THEN 'not self canonical'
                ELSE 'unknown reason'
            END
        ELSE NULL
    END as indexability_reason,
    u.kind as type,
    u.classification,
    c.title,
    md.description as meta_description,
    -- Split h1_tags into h1-1 and h1-2, drop others, add count
    (SELECT h1_text FROM (
        SELECT h1_text, ROW_NUMBER() OVER (ORDER BY h1_text) as rn
        FROM (
            SELECT TRIM(value) as h1_text 
            FROM json_each(CASE 
                WHEN c.h1_tags IS NULL OR c.h1_tags = '' THEN '[]'
                WHEN c.h1_tags LIKE '[%' THEN c.h1_tags
                ELSE '["' || REPLACE(REPLACE(c.h1_tags, '"', '\"'), ',', '","') || '"]'
            END)
            WHERE TRIM(value) != ''
        )
    ) WHERE rn = 1) as h1_1,
    (SELECT h1_text FROM (
        SELECT h1_text, ROW_NUMBER() OVER (ORDER BY h1_text) as rn
        FROM (
            SELECT TRIM(value) as h1_text 
            FROM json_each(CASE 
                WHEN c.h1_tags IS NULL OR c.h1_tags = '' THEN '[]'
                WHEN c.h1_tags LIKE '[%' THEN c.h1_tags
                ELSE '["' || REPLACE(REPLACE(c.h1_tags, '"', '\"'), ',', '","') || '"]'
            END)
            WHERE TRIM(value) != ''
        )
    ) WHERE rn = 2) as h1_2,
    (SELECT COUNT(*) FROM (
        SELECT TRIM(value) as h1_text 
        FROM json_each(CASE 
            WHEN c.h1_tags IS NULL OR c.h1_tags = '' THEN '[]'
            WHEN c.h1_tags LIKE '[%' THEN c.h1_tags
            ELSE '["' || REPLACE(REPLACE(c.h1_tags, '"', '\"'), ',', '","') || '"]'
        END)
        WHERE TRIM(value) != ''
    )) as h1_count,
    -- Convert h2_tags to CSV string and add count
    (SELECT GROUP_CONCAT(TRIM(value), ', ') FROM (
        SELECT TRIM(value) as value
        FROM json_each(CASE 
            WHEN c.h2_tags IS NULL OR c.h2_tags = '' THEN '[]'
            WHEN c.h2_tags LIKE '[%' THEN c.h2_tags
            ELSE '["' || REPLACE(REPLACE(c.h2_tags, '"', '\"'), ',', '","') || '"]'
        END)
        WHERE TRIM(value) != ''
    )) as h2_tags,
    (SELECT COUNT(*) FROM (
        SELECT TRIM(value) as h2_text 
        FROM json_each(CASE 
            WHEN c.h2_tags IS NULL OR c.h2_tags = '' THEN '[]'
            WHEN c.h2_tags LIKE '[%' THEN c.h2_tags
            ELSE '["' || REPLACE(REPLACE(c.h2_tags, '"', '\"'), ',', '","') || '"]'
        END)
        WHERE TRIM(value) != ''
    )) as h2_count,
    c.word_count,
    hl.language_code as html_lang,
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
    COALESCE(i.robots_txt_directives, '') as robots_txt_directives,
    COALESCE(i.html_meta_directives, '') as html_meta_directives,
    COALESCE(i.http_header_directives, '') as http_header_directives,
    GROUP_CONCAT(DISTINCT canonical_urls_table.url) as canonical_urls,
    GROUP_CONCAT(DISTINCT cu.source) as canonical_sources,
    redirect_dest.url as redirect_destination_url,
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
LEFT JOIN page_metadata pm ON u.id = pm.url_id
LEFT JOIN urls redirect_dest ON pm.redirect_destination_url_id = redirect_dest.id
LEFT JOIN frontier f ON u.id = f.url_id
LEFT JOIN meta_descriptions md ON c.meta_description_id = md.id
LEFT JOIN html_languages hl ON c.html_lang_id = hl.id
LEFT JOIN indexability i ON u.id = i.url_id
LEFT JOIN canonical_urls cu ON u.id = cu.url_id
LEFT JOIN urls canonical_urls_table ON cu.canonical_url_id = canonical_urls_table.id
WHERE u.classification IN ('internal', 'network')  -- Only show internal and network URLs
GROUP BY u.id;

-- View for all links from internal pages (internal, network, and external targets)
-- Only shows links from URLs that were actually crawled (have frontier status)
CREATE VIEW IF NOT EXISTS view_internal_links AS
SELECT 
    u1.url as source_url,
    u2.url as target_url,
    u2.classification as target_classification,
    at.text as anchor_text,
    CASE WHEN at.text LIKE '[IMG:%' THEN 1 ELSE 0 END as is_image,
    x.xpath,
    href_urls.url as href,
    href_urls.classification as href_classification,
    il.url_fragment,
    il.url_parameters,
    il.discovered_at
FROM internal_links il
JOIN urls u1 ON il.source_url_id = u1.id
JOIN frontier f1 ON u1.id = f1.url_id  -- Only show links from URLs that were actually crawled
LEFT JOIN urls u2 ON il.target_url_id = u2.id
LEFT JOIN anchor_texts at ON il.anchor_text_id = at.id
LEFT JOIN xpaths x ON il.xpath_id = x.id
LEFT JOIN urls href_urls ON il.href_url_id = href_urls.id;

-- View for internal-to-network links only
-- Only shows links from URLs that were actually crawled (have frontier status)
CREATE VIEW IF NOT EXISTS view_network_links AS
SELECT 
    u1.url as source_url,
    u2.url as target_url,
    u2.classification as target_classification,
    at.text as anchor_text,
    CASE WHEN at.text LIKE '[IMG:%' THEN 1 ELSE 0 END as is_image,
    x.xpath,
    href_urls.url as href,
    href_urls.classification as href_classification,
    il.url_fragment,
    il.url_parameters,
    il.discovered_at
FROM internal_links il
JOIN urls u1 ON il.source_url_id = u1.id
JOIN frontier f1 ON u1.id = f1.url_id  -- Only show links from URLs that were actually crawled
LEFT JOIN urls u2 ON il.target_url_id = u2.id
LEFT JOIN anchor_texts at ON il.anchor_text_id = at.id
LEFT JOIN xpaths x ON il.xpath_id = x.id
LEFT JOIN urls href_urls ON il.href_url_id = href_urls.id
WHERE u2.classification = 'network';  -- Only internal-to-network links

-- View for internal-to-external links only
-- Only shows links from URLs that were actually crawled (have frontier status)
CREATE VIEW IF NOT EXISTS view_external_links AS
SELECT 
    u1.url as source_url,
    u2.url as target_url,
    u2.classification as target_classification,
    at.text as anchor_text,
    CASE WHEN at.text LIKE '[IMG:%' THEN 1 ELSE 0 END as is_image,
    x.xpath,
    href_urls.url as href,
    href_urls.classification as href_classification,
    il.url_fragment,
    il.url_parameters,
    il.discovered_at
FROM internal_links il
JOIN urls u1 ON il.source_url_id = u1.id
JOIN frontier f1 ON u1.id = f1.url_id  -- Only show links from URLs that were actually crawled
LEFT JOIN urls u2 ON il.target_url_id = u2.id
LEFT JOIN anchor_texts at ON il.anchor_text_id = at.id
LEFT JOIN xpaths x ON il.xpath_id = x.id
LEFT JOIN urls href_urls ON il.href_url_id = href_urls.id
WHERE u2.classification = 'external';  -- Only internal-to-external links

-- View for sitemap statistics
CREATE VIEW IF NOT EXISTS view_sitemap_statistics AS
SELECT 
    s.sitemap_url,
    COUNT(*) as total_urls,
    COUNT(DISTINCT us.url_id) as unique_urls,
    MIN(us.discovered_at) as first_discovered,
    MAX(us.discovered_at) as last_discovered,
    -- Count URLs by status
    COUNT(CASE WHEN f.status = 'done' THEN 1 END) as crawled_urls,
    COUNT(CASE WHEN f.status = 'queued' THEN 1 END) as queued_urls,
    COUNT(CASE WHEN f.status IS NULL THEN 1 END) as not_in_frontier
FROM sitemaps s
LEFT JOIN url_sitemaps us ON s.id = us.sitemap_id
LEFT JOIN frontier f ON us.url_id = f.url_id
GROUP BY s.sitemap_url
ORDER BY total_urls DESC;

-- View for comprehensive schema analysis (normalized structure)
CREATE VIEW IF NOT EXISTS view_schema_analysis AS
SELECT 
    u.url,
    st.type_name as main_entity_type,
    si.raw_data as main_entity_data,
    si.is_valid as main_entity_valid,
    si.validation_errors as main_entity_errors,
    si.severity as main_entity_severity,
    psr.position as main_entity_position,
    psr.discovered_at as main_entity_discovered_at,
    
    -- Count of all schema instances on this page
    (SELECT COUNT(psr2.schema_instance_id) 
     FROM page_schema_references psr2 
     WHERE psr2.url_id = u.id) as total_schema_count,
    
    -- Count of main entities
    (SELECT COUNT(*) 
     FROM page_schema_references psr3 
     WHERE psr3.url_id = u.id AND psr3.is_main_entity = 1) as main_entity_count,
    
    -- Count of related entities (properties)
    (SELECT COUNT(*) 
     FROM page_schema_references psr4 
     WHERE psr4.url_id = u.id AND psr4.is_main_entity = 0) as related_entity_count,
    
    -- List of all schema types on this page
    (SELECT GROUP_CONCAT(st2.type_name, ', ') 
     FROM page_schema_references psr5
     JOIN schema_instances si2 ON psr5.schema_instance_id = si2.id
     JOIN schema_types st2 ON si2.schema_type_id = st2.id
     WHERE psr5.url_id = u.id) as all_schema_types,
    
    -- List of property names
    (SELECT GROUP_CONCAT(psr6.property_name, ', ') 
     FROM page_schema_references psr6
     WHERE psr6.url_id = u.id AND psr6.property_name IS NOT NULL) as property_names

FROM urls u
LEFT JOIN page_schema_references psr ON u.id = psr.url_id AND psr.is_main_entity = 1
LEFT JOIN schema_instances si ON psr.schema_instance_id = si.id
LEFT JOIN schema_types st ON si.schema_type_id = st.id
WHERE u.id IN (SELECT url_id FROM page_schema_references)
ORDER BY u.url, psr.position;

-- View for hierarchical schema relationships
CREATE VIEW IF NOT EXISTS view_schema_hierarchy AS
SELECT 
    u.url,
    st.type_name as schema_type,
    si.raw_data as schema_data,
    si.is_valid,
    si.validation_errors,
    si.severity,
    psr.position,
    psr.is_main_entity,
    psr.property_name,
    psr.parent_entity_id,
    
    -- Parent entity info
    parent_st.type_name as parent_entity_type,
    parent_si.raw_data as parent_entity_data,
    
    -- Child entities count
    (SELECT COUNT(*) 
     FROM page_schema_references child_psr
     WHERE child_psr.parent_entity_id = psr.schema_instance_id) as child_count,
    
    -- Sibling entities count (same parent)
    (SELECT COUNT(*) 
     FROM page_schema_references sibling_psr
     WHERE sibling_psr.parent_entity_id = psr.parent_entity_id 
     AND sibling_psr.schema_instance_id != psr.schema_instance_id) as sibling_count

FROM urls u
JOIN page_schema_references psr ON u.id = psr.url_id
JOIN schema_instances si ON psr.schema_instance_id = si.id
JOIN schema_types st ON si.schema_type_id = st.id
LEFT JOIN schema_instances parent_si ON psr.parent_entity_id = parent_si.id
LEFT JOIN schema_types parent_st ON parent_si.schema_type_id = parent_st.id
ORDER BY u.url, psr.is_main_entity DESC, psr.position;

-- Comprehensive crawl status view
CREATE VIEW IF NOT EXISTS view_crawl_status AS
WITH sitemap_stats AS (
    SELECT 
        COUNT(DISTINCT s.id) as sitemaps_scraped,
        COUNT(*) as urls_in_sitemaps
    FROM sitemaps s
    JOIN url_sitemaps us ON s.id = us.sitemap_id
),
url_classification_stats AS (
    SELECT 
        classification,
        COUNT(*) as count
    FROM urls
    GROUP BY classification
),
crawled_stats AS (
    SELECT 
        COUNT(*) as total_crawled,
        COUNT(*) as status_200,  -- URLs with content are considered successfully crawled
        0 as non_200  -- We don't track non-200s in this schema
    FROM content
),
canonical_stats AS (
    SELECT 
        COUNT(*) as total_with_canonical,
        SUM(CASE WHEN cu.canonical_url_id IS NOT NULL THEN 1 ELSE 0 END) as has_canonical,
        SUM(CASE WHEN cu.canonical_url_id IS NULL THEN 1 ELSE 0 END) as no_canonical
    FROM content c
    LEFT JOIN canonical_urls cu ON c.url_id = cu.url_id
),
indexability_stats AS (
    SELECT 
        COUNT(*) as total_indexable_checked,
        SUM(CASE WHEN overall_indexable = 1 THEN 1 ELSE 0 END) as indexable,
        SUM(CASE WHEN overall_indexable = 0 THEN 1 ELSE 0 END) as non_indexable
    FROM indexability
),
sitemap_coverage AS (
    SELECT 
        COUNT(DISTINCT u.id) as internal_urls_total,
        COUNT(DISTINCT us.url_id) as internal_urls_in_sitemap,
        COUNT(DISTINCT u.id) - COUNT(DISTINCT us.url_id) as internal_urls_not_in_sitemap
    FROM urls u
    LEFT JOIN url_sitemaps us ON u.id = us.url_id
    WHERE u.classification IN ('internal', 'network')
),
sitemap_orphans AS (
    SELECT 
        COUNT(DISTINCT us.url_id) as sitemap_urls_total,
        COUNT(DISTINCT c.url_id) as sitemap_urls_crawled,
        COUNT(DISTINCT us.url_id) - COUNT(DISTINCT c.url_id) as sitemap_urls_not_crawled
    FROM url_sitemaps us
    LEFT JOIN content c ON us.url_id = c.url_id
)
SELECT 
    -- Sitemap statistics
    ss.sitemaps_scraped,
    ss.urls_in_sitemaps,
    
    -- URL classification
    COALESCE(SUM(CASE WHEN ucs.classification = 'internal' THEN ucs.count ELSE 0 END), 0) as internal_urls,
    COALESCE(SUM(CASE WHEN ucs.classification = 'network' THEN ucs.count ELSE 0 END), 0) as network_urls,
    COALESCE(SUM(CASE WHEN ucs.classification = 'external' THEN ucs.count ELSE 0 END), 0) as external_urls,
    COALESCE(SUM(CASE WHEN ucs.classification = 'social' THEN ucs.count ELSE 0 END), 0) as social_urls,
    
    -- Crawl progress
    cs.total_crawled,
    cs.status_200,
    cs.non_200,
    
    -- Canonical URL analysis
    cans.has_canonical,
    cans.no_canonical,
    
    -- Indexability analysis
    ins.indexable,
    ins.non_indexable,
    
    -- Sitemap coverage analysis
    sc.internal_urls_not_in_sitemap,
    so.sitemap_urls_not_crawled,
    
    -- Calculated percentages
    ROUND((cs.status_200 * 100.0 / NULLIF(cs.total_crawled, 0)), 2) as success_rate_percent,
    ROUND((cans.has_canonical * 100.0 / NULLIF(cans.total_with_canonical, 0)), 2) as canonical_coverage_percent,
    ROUND((ins.indexable * 100.0 / NULLIF(ins.total_indexable_checked, 0)), 2) as indexability_rate_percent,
    ROUND((sc.internal_urls_in_sitemap * 100.0 / NULLIF(sc.internal_urls_total, 0)), 2) as sitemap_coverage_percent

FROM sitemap_stats ss
CROSS JOIN url_classification_stats ucs
CROSS JOIN crawled_stats cs
CROSS JOIN canonical_stats cans
CROSS JOIN indexability_stats ins
CROSS JOIN sitemap_coverage sc
CROSS JOIN sitemap_orphans so
GROUP BY 
    ss.sitemaps_scraped, ss.urls_in_sitemaps,
    cs.total_crawled, cs.status_200, cs.non_200,
    cans.has_canonical, cans.no_canonical,
    ins.indexable, ins.non_indexable,
    sc.internal_urls_not_in_sitemap, so.sitemap_urls_not_crawled;

--- Enhanced views for comprehensive link analysis with both original and normalized URLs

-- Enhanced view showing original link data with normalized targets
CREATE VIEW IF NOT EXISTS view_internal_links_enhanced AS
SELECT 
    u1.url as source_url,
    u2.url as target_url_normalized,        -- Normalized URL (for crawling)
    href_urls.url as target_url_original,   -- Original URL (for analysis)
    u2.classification as target_classification,
    at.text as anchor_text,
    CASE WHEN at.text LIKE '[IMG:%' THEN 1 ELSE 0 END as is_image,
    x.xpath,
    il.url_fragment,                        -- Original fragment
    il.url_parameters,                      -- Original parameters
    il.discovered_at,
    -- Additional analysis fields
    CASE 
        WHEN il.url_fragment IS NOT NULL AND il.url_fragment != '' THEN 1 
        ELSE 0 
    END as has_fragment,
    CASE 
        WHEN il.url_parameters IS NOT NULL AND il.url_parameters != '' THEN 1 
        ELSE 0 
    END as has_parameters,
    CASE 
        WHEN href_urls.url != u2.url THEN 1 
        ELSE 0 
    END as url_was_normalized
FROM internal_links il
JOIN urls u1 ON il.source_url_id = u1.id
JOIN frontier f1 ON u1.id = f1.url_id  -- Only show links from URLs that were actually crawled
LEFT JOIN urls u2 ON il.target_url_id = u2.id           -- Normalized target
LEFT JOIN urls href_urls ON il.href_url_id = href_urls.id  -- Original href
LEFT JOIN anchor_texts at ON il.anchor_text_id = at.id
LEFT JOIN xpaths x ON il.xpath_id = x.id;

-- View for UTM parameter analysis
CREATE VIEW IF NOT EXISTS view_utm_links AS
SELECT 
    u1.url as source_url,
    href_urls.url as target_url_original,
    at.text as anchor_text,
    il.url_parameters,
    il.discovered_at
FROM internal_links il
JOIN urls u1 ON il.source_url_id = u1.id
JOIN frontier f1 ON u1.id = f1.url_id
LEFT JOIN urls href_urls ON il.href_url_id = href_urls.id
LEFT JOIN anchor_texts at ON il.anchor_text_id = at.id
WHERE il.url_parameters LIKE '%utm_%';

-- View for fragment/anchor link analysis
CREATE VIEW IF NOT EXISTS view_anchor_links AS
SELECT 
    u1.url as source_url,
    href_urls.url as target_url_original,
    at.text as anchor_text,
    il.url_fragment,
    il.discovered_at
FROM internal_links il
JOIN urls u1 ON il.source_url_id = u1.id
JOIN frontier f1 ON u1.id = f1.url_id
LEFT JOIN urls href_urls ON il.href_url_id = href_urls.id
LEFT JOIN anchor_texts at ON il.anchor_text_id = at.id
WHERE il.url_fragment IS NOT NULL AND il.url_fragment != '';
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
        # Use a more robust approach to split SQL statements
        import re
        
        # Remove comments and split by semicolon
        schema_clean = re.sub(r'--.*$', '', CRAWL_SCHEMA, flags=re.MULTILINE)
        statements = [stmt.strip() for stmt in schema_clean.split(';') if stmt.strip()]
        
        # Execute each statement
        for stmt in statements:
            if stmt:
                await db.execute(stmt)
        await db.commit()

# ------------------ URL classification ------------------

def classify_url(url: str, base_domain: str, is_from_sitemap: bool = False, is_from_hreflang: bool = False) -> str:
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
    
    # Network URLs are those found in hreflang (sitemap, HTTP headers, or page head)
    if is_from_hreflang:
        return 'network'
    
    # Everything else is external
    return 'external'

# ------------------ URL ID management ------------------

async def get_or_create_url_id(url: str, base_domain: str, db_path: str = CRAWL_DB_PATH, conn: aiosqlite.Connection = None, is_from_hreflang: bool = False) -> int:
    """Get URL ID, creating the URL record if it doesn't exist."""
    if conn:
        # Use existing connection
        cursor = await conn.execute("SELECT id FROM urls WHERE url = ?", (url,))
        row = await cursor.fetchone()
        if row:
            return row[0]
        
        # Classify the URL
        classification = classify_url(url, base_domain, is_from_hreflang=is_from_hreflang)
        
        # Create new URL record
        cursor = await conn.execute(
            "INSERT INTO urls (url, classification, first_seen, last_seen) VALUES (?, ?, ?, ?)",
            (url, classification, int(time.time()), int(time.time()))
        )
        return cursor.lastrowid
    else:
        # Create new connection
        async with aiosqlite.connect(db_path) as db:
            # Try to get existing URL ID
            cursor = await db.execute("SELECT id FROM urls WHERE url = ?", (url,))
            row = await cursor.fetchone()
            if row:
                return row[0]
            
            # Classify the URL
            classification = classify_url(url, base_domain, is_from_hreflang=is_from_hreflang)
            
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

async def get_conditional_headers(url: str, base_domain: str, pages_db_path: str = PAGES_DB_PATH, crawl_db_path: str = CRAWL_DB_PATH) -> tuple[str | None, str | None]:
    """Get ETag and Last-Modified headers for a URL from previous crawl."""
    try:
        # Get URL ID
        url_id = await get_or_create_url_id(url, base_domain, crawl_db_path)
        
        async with aiosqlite.connect(pages_db_path) as db:
            cursor = await db.execute(
                "SELECT etag, last_modified FROM pages WHERE url_id = ? AND etag IS NOT NULL AND last_modified IS NOT NULL",
                (url_id,)
            )
            row = await cursor.fetchone()
            if row:
                return row[0], row[1]  # etag, last_modified
    except Exception:
        pass  # Return None, None if any error occurs
    
    return None, None

# ------------------ writers ------------------

async def write_page(url: str, final_url: str, status: int, headers: dict, html: str, base_domain: str, pages_db_path: str = PAGES_DB_PATH, crawl_db_path: str = CRAWL_DB_PATH, redirect_chain_json: str = None):
    now = int(time.time())
    
    # Get URL IDs
    url_id = await get_or_create_url_id(url, base_domain, crawl_db_path)
    final_url_id = await get_or_create_url_id(final_url, base_domain, crawl_db_path) if final_url != url else url_id
    
    # Extract initial status code and redirect destination from redirect chain
    initial_status_code = status  # Default to final status if no redirect chain
    redirect_destination_url_id = None
    
    if redirect_chain_json:
        try:
            redirect_chain = json.loads(redirect_chain_json)
            if redirect_chain:
                # First step in chain is the initial request
                initial_status_code = redirect_chain[0].get('status', status)
                
                # If there's a redirect (301, 302, etc.), find the destination
                if initial_status_code in [301, 302, 303, 307, 308]:
                    # Look for Location header in the first redirect response
                    first_response_headers = redirect_chain[0].get('headers', {})
                    location = first_response_headers.get('location')
                    if location:
                        # Normalize the redirect destination URL
                        from urllib.parse import urljoin
                        redirect_destination = urljoin(url, location)
                        redirect_destination_url_id = await get_or_create_url_id(redirect_destination, base_domain, crawl_db_path)
        except (json.JSONDecodeError, KeyError, IndexError):
            # If parsing fails, use defaults
            pass
    
    # Extract ETag and Last-Modified from headers
    etag = headers.get('etag', '').strip('"') if headers.get('etag') else None
    last_modified = headers.get('last-modified', '').strip() if headers.get('last-modified') else None
    
    # Store HTML and headers in pages database
    async with aiosqlite.connect(pages_db_path) as db:
        await db.execute(
            """
        INSERT INTO pages(url_id, headers_json, html_compressed)
        VALUES (?,?,?)
        ON CONFLICT(url_id) DO UPDATE SET
          headers_json=excluded.headers_json,
          html_compressed=excluded.html_compressed
        """,
            (url_id, json.dumps(headers, ensure_ascii=False), compress_html(html)),
        )
        await db.commit()
    
    # Store metadata in crawl database
    async with aiosqlite.connect(crawl_db_path) as db:
        await db.execute(
            """
        INSERT INTO page_metadata(url_id, initial_status_code, final_status_code, final_url_id, redirect_destination_url_id, fetched_at, etag, last_modified)
        VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT(url_id) DO UPDATE SET
          initial_status_code=excluded.initial_status_code,
          final_status_code=excluded.final_status_code,
          final_url_id=excluded.final_url_id,
          redirect_destination_url_id=excluded.redirect_destination_url_id,
          fetched_at=excluded.fetched_at,
          etag=excluded.etag,
          last_modified=excluded.last_modified
        """,
            (url_id, initial_status_code, status, final_url_id, redirect_destination_url_id, now, etag, last_modified),
        )
        await db.commit()

async def upsert_url(url: str, kind: str, base_domain: str, discovered_from: Optional[str] = None, is_from_hreflang: bool = False, db_path: str = CRAWL_DB_PATH):
    now = int(time.time())
    
    # Get discovered_from_id if provided
    discovered_from_id = None
    if discovered_from:
        discovered_from_id = await get_or_create_url_id(discovered_from, base_domain, db_path)
    
    # Classify the URL
    classification = classify_url(url, base_domain, is_from_hreflang=is_from_hreflang)
    
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

async def batch_write_pages(pages_data: List[Tuple[str, str, int, dict, str, str, str]], pages_db_path: str = PAGES_DB_PATH, crawl_db_path: str = CRAWL_DB_PATH, batch_size: int = 50):
    """Batch write multiple pages for better performance."""
    if not pages_data:
        return
    
    # Process in smaller batches to avoid timeouts
    for i in range(0, len(pages_data), batch_size):
        batch = pages_data[i:i + batch_size]
        await _batch_write_pages_chunk(batch, pages_db_path, crawl_db_path)

async def _batch_write_pages_chunk(pages_data: List[Tuple[str, str, int, dict, str, str, str]], pages_db_path: str, crawl_db_path: str):
    """Write a chunk of pages."""
    
    async with aiosqlite.connect(pages_db_path) as pages_conn, aiosqlite.connect(crawl_db_path) as crawl_conn:
        # Prepare batch data for pages (HTML and headers only)
        pages_batch_data = []
        metadata_batch_data = []
        
        for url, final_url, status, headers, html, base_domain, redirect_chain_json in pages_data:
            # Get URL IDs
            url_id = await get_or_create_url_id_with_conn(url, base_domain, crawl_db_path, crawl_conn)
            final_url_id = await get_or_create_url_id_with_conn(final_url, base_domain, crawl_db_path, crawl_conn) if final_url != url else url_id
            
            # Extract initial status code and redirect destination from redirect chain
            initial_status_code = status  # Default to final status if no redirect chain
            redirect_destination_url_id = None
            
            if redirect_chain_json:
                try:
                    redirect_chain = json.loads(redirect_chain_json)
                    if redirect_chain:
                        # First step in chain is the initial request
                        initial_status_code = redirect_chain[0].get('status', status)
                        
                        # If there's a redirect (301, 302, etc.), find the destination
                        if initial_status_code in [301, 302, 303, 307, 308]:
                            # Look for Location header in the first redirect response
                            first_response_headers = redirect_chain[0].get('headers', {})
                            location = first_response_headers.get('location')
                            if location:
                                # Normalize the redirect destination URL
                                from urllib.parse import urljoin
                                redirect_destination = urljoin(url, location)
                                redirect_destination_url_id = await get_or_create_url_id_with_conn(redirect_destination, base_domain, crawl_db_path, crawl_conn)
                except (json.JSONDecodeError, KeyError, IndexError):
                    # If parsing fails, use defaults
                    pass
            
            # Extract ETag and Last-Modified from headers
            etag = headers.get('etag', '').strip('"') if headers.get('etag') else None
            last_modified = headers.get('last-modified', '').strip() if headers.get('last-modified') else None
            
            # Pages data (HTML and headers only)
            pages_batch_data.append((
                url_id, json.dumps(headers, ensure_ascii=False), compress_html(html)
            ))
            
            # Metadata data (status, timestamps, etc.)
            metadata_batch_data.append((
                url_id, initial_status_code, status, final_url_id, redirect_destination_url_id, int(time.time()), etag, last_modified
            ))
        
        # Batch insert pages (HTML and headers)
        await pages_conn.executemany(
            """
            INSERT INTO pages(url_id, headers_json, html_compressed)
            VALUES (?,?,?)
            ON CONFLICT(url_id) DO UPDATE SET
              headers_json=excluded.headers_json,
              html_compressed=excluded.html_compressed
            """,
            pages_batch_data
        )
        await pages_conn.commit()
        
        # Batch insert metadata
        await crawl_conn.executemany(
            """
            INSERT INTO page_metadata(url_id, initial_status_code, final_status_code, final_url_id, redirect_destination_url_id, fetched_at, etag, last_modified)
            VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(url_id) DO UPDATE SET
              initial_status_code=excluded.initial_status_code,
              final_status_code=excluded.final_status_code,
              final_url_id=excluded.final_url_id,
              redirect_destination_url_id=excluded.redirect_destination_url_id,
              fetched_at=excluded.fetched_at,
              etag=excluded.etag,
              last_modified=excluded.last_modified
            """,
            metadata_batch_data
        )
        await crawl_conn.commit()

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

async def add_hreflang_urls_to_frontier(crawl_db_path: str, base_domain: str):
    """Add hreflang URLs to the frontier for crawling."""
    async with aiosqlite.connect(crawl_db_path) as conn:
        # Get all hreflang URLs from both HTML head and sitemap that are not already in the frontier
        cursor = await conn.execute("""
            SELECT DISTINCT u.url 
            FROM (
                SELECT href_url_id FROM hreflang_html_head
                UNION
                SELECT href_url_id FROM hreflang_sitemap
            ) hreflang_urls
            JOIN urls u ON hreflang_urls.href_url_id = u.id
            WHERE u.id NOT IN (SELECT url_id FROM frontier)
        """)
        hreflang_urls = await cursor.fetchall()
        
        if hreflang_urls:
            print(f"Adding {len(hreflang_urls)} hreflang URLs to frontier...")
            for (url,) in hreflang_urls:
                await frontier_seed(url, base_domain, reset=False, db_path=crawl_db_path, depth=0)

async def batch_write_content_with_url_resolution(content_data: List[Tuple[str, dict, str]], crawl_db_path: str):
    """Write content data with URL ID resolution and normalized tables."""
    if not content_data:
        return
    
    # Retry logic for database locks
    for attempt in range(3):
        try:
            async with aiosqlite.connect(crawl_db_path, timeout=30.0) as conn:
                for url, content_info, base_domain in content_data:
                    # Get URL ID
                    cursor = await conn.execute("SELECT id FROM urls WHERE url = ?", (url,))
                    row = await cursor.fetchone()
                    if not row:
                        continue
                    
                    url_id = row[0]
                    
                    # Get or create normalized IDs
                    meta_description_id = await get_or_create_meta_description_id(content_info['meta_description'], conn)
                    html_lang_id = await get_or_create_html_language_id(content_info['html_lang'], conn)
                    
                    # Insert/update content
                    await conn.execute(
                        """
                        INSERT INTO content(url_id, title, meta_description_id, h1_tags, h2_tags, word_count, html_lang_id)
                        VALUES (?,?,?,?,?,?,?)
                        ON CONFLICT(url_id) DO UPDATE SET
                          title=excluded.title,
                          meta_description_id=excluded.meta_description_id,
                          h1_tags=excluded.h1_tags,
                          h2_tags=excluded.h2_tags,
                          word_count=excluded.word_count,
                          html_lang_id=excluded.html_lang_id
                        """,
                        (
                            url_id,
                            content_info['title'],
                            meta_description_id,
                            json.dumps(content_info['h1_tags'], ensure_ascii=False),
                            json.dumps(content_info['h2_tags'], ensure_ascii=False),
                            content_info['word_count'],
                            html_lang_id
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
                        canonical_url = content_info['canonical_url']
                        
                        # Normalize protocol-relative URLs
                        normalized_canonical_url = canonical_url
                        if canonical_url.startswith('//'):
                            # Convert protocol-relative URL to use the same protocol as the base domain
                            from urllib.parse import urlparse
                            if base_domain:
                                # Use the base domain's protocol
                                base_protocol = 'https'  # Default to https for modern sites
                                normalized_canonical_url = f"{base_protocol}:{canonical_url}"
                            else:
                                # Fallback to https
                                normalized_canonical_url = f"https:{canonical_url}"
                        
                        canonical_url_id = await get_or_create_canonical_url_id(normalized_canonical_url, base_domain, conn)
                        await conn.execute(
                            """
                            INSERT OR IGNORE INTO canonical_urls(url_id, canonical_url_id, source)
                            VALUES (?, ?, 'html_head')
                            """,
                            (url_id, canonical_url_id)
                        )
                    
                    # Process hreflang URLs from HTML head
                    if content_info.get('hreflang_urls'):
                        for hreflang_data in content_info['hreflang_urls']:
                            hreflang_url = hreflang_data['url']
                            hreflang_lang = hreflang_data['hreflang']
                            
                            # Normalize protocol-relative URLs
                            normalized_hreflang_url = hreflang_url
                            if hreflang_url.startswith('//'):
                                # Convert protocol-relative URL to use the same protocol as the base domain
                                from urllib.parse import urlparse
                                if base_domain:
                                    # Use the base domain's protocol
                                    base_protocol = 'https'  # Default to https for modern sites
                                    normalized_hreflang_url = f"{base_protocol}:{hreflang_url}"
                                else:
                                    # Fallback to https
                                    normalized_hreflang_url = f"https:{hreflang_url}"
                            
                            # Get or create hreflang language ID
                            hreflang_lang_id = await get_or_create_hreflang_language_id(hreflang_lang, conn)
                            
                            # Get or create target URL ID (classify as network since it's from hreflang)
                            target_url_id = await get_or_create_url_id(normalized_hreflang_url, base_domain, crawl_db_path, conn, is_from_hreflang=True)
                            
                            # Insert hreflang HTML head data
                            await conn.execute(
                                """
                                INSERT OR IGNORE INTO hreflang_html_head(url_id, hreflang_id, href_url_id)
                                VALUES (?, ?, ?)
                                """,
                                (url_id, hreflang_lang_id, target_url_id)
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
                    
                    # Check if URL is self-canonical
                    cursor = await conn.execute("SELECT canonical_url_id FROM canonical_urls WHERE url_id = ?", (url_id,))
                    canonical_row = await cursor.fetchone()
                    is_self_canonical = canonical_row and canonical_row[0] == url_id
                    
                    # Get initial status code from page_metadata table in crawl database
                    cursor = await conn.execute("SELECT initial_status_code FROM page_metadata WHERE url_id = ?", (url_id,))
                    page_row = await cursor.fetchone()
                    initial_status_code = page_row[0] if page_row else None
                    
                    # Calculate overall indexability: only true if initial_status=200, self canonical, and no restrictions
                    overall_indexable = (
                        initial_status_code == 200 and 
                        is_self_canonical and 
                        robots_txt_allows and 
                        html_meta_allows and 
                        http_header_allows
                    )
                    
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
                            overall_indexable
                        )
                    )
                    
                    # Process schema data if present - use new normalized structure
                    if content_info.get('schema_data'):
                        # Use the new normalized schema storage with existing connection
                        await create_page_schema_references_with_conn(url_id, content_info['schema_data'], conn, crawl_db_path)
                
                await conn.commit()
                break  # Success, exit retry loop
                
        except aiosqlite.OperationalError as e:
            if "database is locked" in str(e) and attempt < 2:
                import asyncio
                await asyncio.sleep(0.1 * (attempt + 1))  # Exponential backoff
                continue
            raise

async def batch_write_internal_links(links_data: List[Tuple[str, list, str]], crawl_db_path: str):
    """Write internal links data with normalized references and URL components."""
    if not links_data:
        return
    
    # Retry logic for database locks
    for attempt in range(3):
        try:
            async with aiosqlite.connect(crawl_db_path, timeout=30.0) as conn:
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
                        # Get both normalized and original URLs
                        target_url = link_info['url']  # Normalized URL for crawling
                        original_href = link_info.get('original_href', link_info['href'])  # Original href for analysis
                        href_original = link_info['href']  # Original relative href
                        
                        # Parse URL components from original href
                        url_components = parse_url_components(href_original, source_url)
                        
                        # Get or create normalized IDs
                        anchor_text_id = await get_or_create_anchor_text_id(link_info['anchor_text'], conn)
                        xpath_id = await get_or_create_xpath_id(link_info['xpath'], conn)
                        
                        # Store ORIGINAL href URL (for link analysis)
                        href_url_id = await get_or_create_href_url_id(original_href, base_domain, conn)
                        
                        # Try to get NORMALIZED target URL ID (for crawling)
                        target_url_id = None
                        if target_url:
                            cursor = await conn.execute("SELECT id FROM urls WHERE url = ?", (target_url,))
                            row = await cursor.fetchone()
                            if row:
                                target_url_id = row[0]
                        
                        # Classify the link using normalized URL
                        classification = classify_url(target_url, base_domain)
                        
                        # Store ALL links from internal pages in internal_links table
                        # This allows us to see all links from internal pages, including those with image alt text
                        # that point to external URLs
                        await conn.execute(
                            """
                            INSERT OR IGNORE INTO internal_links(
                                source_url_id, target_url_id, anchor_text_id, xpath_id, href_url_id,
                                url_fragment, url_parameters, discovered_at
                            )
                            VALUES (?,?,?,?,?,?,?,?)
                            """,
                            (
                                source_url_id,
                                target_url_id,  # Normalized URL ID for crawling
                                anchor_text_id,
                                xpath_id,
                                href_url_id,    # Original href URL ID for analysis
                                link_info.get('fragment', url_components['url_fragment']),  # Use new fragment if available
                                link_info.get('parameters', url_components['url_parameters']),  # Use new parameters if available
                                now
                            )
                        )
                        
                        # Count internal vs external for statistics (use normalized URLs)
                        if classification == 'internal':
                            internal_count += 1
                            internal_unique.add(target_url)
                        else:
                            external_count += 1
                            external_unique.add(target_url)
                    
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
                break  # Success, exit retry loop
                
        except aiosqlite.OperationalError as e:
            if "database is locked" in str(e) and attempt < 2:
                import asyncio
                await asyncio.sleep(0.1 * (attempt + 1))  # Exponential backoff
                continue
            raise

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

async def get_or_create_href_url_id(href: str, base_domain: str, conn: aiosqlite.Connection) -> int:
    """Get or create href URL ID in the urls table."""
    import time
    
    # Normalize protocol-relative URLs
    normalized_href = href
    if href.startswith('//'):
        # Convert protocol-relative URL to use the same protocol as the base domain
        from urllib.parse import urlparse
        if base_domain:
            # Use the base domain's protocol
            base_protocol = 'https'  # Default to https for modern sites
            normalized_href = f"{base_protocol}:{href}"
        else:
            # Fallback to https
            normalized_href = f"https:{href}"
    
    # First try to get existing URL ID
    cursor = await conn.execute("SELECT id FROM urls WHERE url = ?", (normalized_href,))
    row = await cursor.fetchone()
    if row:
        return row[0]
    
    # If not found, create new URL entry
    classification = classify_url(normalized_href, base_domain)
    now = int(time.time())
    cursor = await conn.execute(
        "INSERT INTO urls (url, classification, first_seen, last_seen) VALUES (?, ?, ?, ?)",
        (normalized_href, classification, now, now)
    )
    return cursor.lastrowid

async def get_or_create_canonical_url_id(canonical_url: str, base_domain: str, conn: aiosqlite.Connection) -> int:
    """Get or create canonical URL ID in the urls table."""
    import time
    
    # Normalize protocol-relative URLs
    normalized_canonical_url = canonical_url
    if canonical_url.startswith('//'):
        # Convert protocol-relative URL to use the same protocol as the base domain
        from urllib.parse import urlparse
        if base_domain:
            # Use the base domain's protocol
            base_protocol = 'https'  # Default to https for modern sites
            normalized_canonical_url = f"{base_protocol}:{canonical_url}"
        else:
            # Fallback to https
            normalized_canonical_url = f"https:{canonical_url}"
    
    # First try to get existing URL ID
    cursor = await conn.execute("SELECT id FROM urls WHERE url = ?", (normalized_canonical_url,))
    row = await cursor.fetchone()
    if row:
        return row[0]
    
    # If not found, create new URL entry
    classification = classify_url(normalized_canonical_url, base_domain)
    now = int(time.time())
    cursor = await conn.execute(
        "INSERT INTO urls (url, classification, first_seen, last_seen) VALUES (?, ?, ?, ?)",
        (normalized_canonical_url, classification, now, now)
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

async def get_or_create_meta_description_id(description: str, conn: aiosqlite.Connection) -> int:
    """Get or create meta description ID."""
    if not description:
        return None
    
    cursor = await conn.execute("SELECT id FROM meta_descriptions WHERE description = ?", (description,))
    row = await cursor.fetchone()
    if row:
        return row[0]
    
    # Create new meta description
    cursor = await conn.execute("INSERT INTO meta_descriptions(description) VALUES (?)", (description,))
    return cursor.lastrowid

async def get_or_create_html_language_id(language_code: str, conn: aiosqlite.Connection) -> int:
    """Get or create HTML language ID."""
    if not language_code:
        return None
    
    cursor = await conn.execute("SELECT id FROM html_languages WHERE language_code = ?", (language_code,))
    row = await cursor.fetchone()
    if row:
        return row[0]
    
    # Create new language code
    cursor = await conn.execute("INSERT INTO html_languages(language_code) VALUES (?)", (language_code,))
    return cursor.lastrowid

async def get_or_create_hreflang_language_id(language_code: str, conn: aiosqlite.Connection) -> int:
    """Get or create hreflang language ID."""
    cursor = await conn.execute("SELECT id FROM hreflang_languages WHERE language_code = ?", (language_code,))
    row = await cursor.fetchone()
    if row:
        return row[0]
    
    # Create new language code
    cursor = await conn.execute("INSERT INTO hreflang_languages(language_code) VALUES (?)", (language_code,))
    return cursor.lastrowid

def should_retry_status_code(status_code: int) -> bool:
    """Determine if a status code should be retried."""
    # Status codes worth retrying:
    # 0 = connection/timeout errors
    # 5xx = server errors (500, 502, 503, 504, 507, 508, etc.)
    # Some 4xx codes that might be temporary
    
    if status_code == 0:
        return True  # Connection/timeout errors
    elif 500 <= status_code < 600:
        return True  # Server errors (500, 502, 503, 504, 507, 508, etc.)
    elif status_code in [408, 423, 429, 451, 420]:
        # Temporary client issues worth retrying:
        # 408 = Request Timeout (server might be slow)
        # 423 = Locked (resource temporarily locked)
        # 429 = Too Many Requests (rate limited)
        # 420 = Enhance Your Calm (Twitter rate limiting)
        # 451 = Unavailable For Legal Reasons (might be temporary geo-blocking)
        return True
    else:
        return False  # Don't retry other 4xx client errors, 3xx redirects, 2xx success

async def record_failed_url(url_id: int, status_code: int, failure_reason: str, conn: aiosqlite.Connection, retry_delay: float = 1.0, backoff_factor: float = 2.0):
    """Record a failed URL for potential retry."""
    import time
    
    now = int(time.time())
    
    # Check if this URL is already in failed_urls
    cursor = await conn.execute("SELECT retry_count FROM failed_urls WHERE url_id = ?", (url_id,))
    row = await cursor.fetchone()
    
    if row:
        # Update existing failed URL
        retry_count = row[0] + 1
        next_retry_delay = retry_delay * (backoff_factor ** retry_count)
        next_retry_at = now + int(next_retry_delay)
        
        await conn.execute(
            """
            UPDATE failed_urls 
            SET status_code = ?, failure_reason = ?, retry_count = ?, 
                last_retry_at = ?, next_retry_at = ?
            WHERE url_id = ?
            """,
            (status_code, failure_reason, retry_count, now, next_retry_at, url_id)
        )
    else:
        # Insert new failed URL
        next_retry_at = now + int(retry_delay)
        await conn.execute(
            """
            INSERT INTO failed_urls (url_id, status_code, failure_reason, retry_count, last_retry_at, next_retry_at, created_at)
            VALUES (?, ?, ?, 0, ?, ?, ?)
            """,
            (url_id, status_code, failure_reason, now, next_retry_at, now)
        )

async def get_urls_ready_for_retry(conn: aiosqlite.Connection, max_retries: int = 3) -> list[tuple[int, str]]:
    """Get URLs that are ready for retry (next_retry_at <= now and retry_count < max_retries)."""
    import time
    
    now = int(time.time())
    cursor = await conn.execute(
        """
        SELECT fu.url_id, u.url 
        FROM failed_urls fu
        JOIN urls u ON fu.url_id = u.id
        WHERE fu.next_retry_at <= ? AND fu.retry_count < ?
        ORDER BY fu.next_retry_at ASC
        """,
        (now, max_retries)
    )
    return await cursor.fetchall()

async def remove_failed_url(url_id: int, conn: aiosqlite.Connection):
    """Remove a URL from the failed_urls table (when it succeeds)."""
    await conn.execute("DELETE FROM failed_urls WHERE url_id = ?", (url_id,))

async def get_retry_statistics(conn: aiosqlite.Connection) -> dict:
    """Get comprehensive retry statistics."""
    import time
    stats = {}
    
    # Total failed URLs
    cursor = await conn.execute("SELECT COUNT(*) FROM failed_urls")
    stats['total_failed'] = (await cursor.fetchone())[0]
    
    # Failed URLs by status code
    cursor = await conn.execute("SELECT status_code, COUNT(*) FROM failed_urls GROUP BY status_code ORDER BY status_code")
    stats['by_status'] = dict(await cursor.fetchall())
    
    # Failed URLs by retry count
    cursor = await conn.execute("SELECT retry_count, COUNT(*) FROM failed_urls GROUP BY retry_count ORDER BY retry_count")
    stats['by_retry_count'] = dict(await cursor.fetchall())
    
    # URLs ready for retry
    cursor = await conn.execute("SELECT COUNT(*) FROM failed_urls WHERE next_retry_at <= ?", (int(time.time()),))
    stats['ready_for_retry'] = (await cursor.fetchone())[0]
    
    return stats

async def get_crawl_status(crawl_db_path: str) -> dict:
    """Get comprehensive crawl status information."""
    async with aiosqlite.connect(crawl_db_path) as db:
        cursor = await db.execute("SELECT * FROM crawl_status LIMIT 1")
        result = await cursor.fetchone()
        
        if result:
            columns = [description[0] for description in cursor.description]
            return dict(zip(columns, result))
        return {}

async def print_crawl_status(crawl_db_path: str):
    """Print a formatted crawl status report."""
    status = await get_crawl_status(crawl_db_path)
    
    if not status:
        print("No crawl status data available.")
        return
    
    print("\n" + "="*80)
    print("                    CRAWL STATUS REPORT")
    print("="*80)
    
    # Sitemap Statistics
    print(f"\n📋 SITEMAP DISCOVERY:")
    print(f"   • Sitemaps scraped: {status.get('sitemaps_scraped', 0):,}")
    print(f"   • URLs in sitemaps: {status.get('urls_in_sitemaps', 0):,}")
    print(f"   • Sitemap coverage: {status.get('sitemap_coverage_percent', 0):.1f}%")
    
    # URL Classification
    print(f"\n🔗 URL DISCOVERY:")
    print(f"   • Internal URLs: {status.get('internal_urls', 0):,}")
    print(f"   • Network URLs: {status.get('network_urls', 0):,}")
    print(f"   • External URLs: {status.get('external_urls', 0):,}")
    print(f"   • Social URLs: {status.get('social_urls', 0):,}")
    
    # Crawl Progress
    print(f"\n🚀 CRAWL PROGRESS:")
    print(f"   • Total crawled: {status.get('total_crawled', 0):,}")
    print(f"   • Status 200: {status.get('status_200', 0):,}")
    print(f"   • Non-200: {status.get('non_200', 0):,}")
    print(f"   • Success rate: {status.get('success_rate_percent', 0):.1f}%")
    
    # Canonical URLs
    print(f"\n🔗 CANONICAL URLS:")
    print(f"   • With canonical: {status.get('has_canonical', 0):,}")
    print(f"   • No canonical: {status.get('no_canonical', 0):,}")
    print(f"   • Canonical coverage: {status.get('canonical_coverage_percent', 0):.1f}%")
    
    # Indexability
    print(f"\n🔍 INDEXABILITY:")
    print(f"   • Indexable: {status.get('indexable', 0):,}")
    print(f"   • Non-indexable: {status.get('non_indexable', 0):,}")
    print(f"   • Indexability rate: {status.get('indexability_rate_percent', 0):.1f}%")
    
    # Sitemap Analysis
    print(f"\n📊 SITEMAP ANALYSIS:")
    print(f"   • Internal URLs not in sitemap: {status.get('internal_urls_not_in_sitemap', 0):,}")
    print(f"   • Sitemap URLs not crawled: {status.get('sitemap_urls_not_crawled', 0):,}")
    
    print("="*80)

async def batch_write_hreflang_sitemap_data(hreflang_data: List[Tuple[str, str, str]], crawl_db_path: str, base_domain: str = None):
    """Write hreflang data from sitemaps to the normalized database structure."""
    if not hreflang_data:
        return
    
    async with aiosqlite.connect(crawl_db_path) as conn:
        for url, hreflang, href_url in hreflang_data:
            # Normalize protocol-relative URLs
            normalized_href_url = href_url
            if href_url.startswith('//'):
                # Convert protocol-relative URL to use the same protocol as the base domain
                from urllib.parse import urlparse
                if base_domain:
                    # Use the base domain's protocol
                    base_protocol = 'https'  # Default to https for modern sites
                    normalized_href_url = f"{base_protocol}:{href_url}"
                else:
                    # Fallback to https
                    normalized_href_url = f"https:{href_url}"
            
            # Get source URL ID
            cursor = await conn.execute("SELECT id FROM urls WHERE url = ?", (url,))
            source_row = await cursor.fetchone()
            if not source_row:
                continue
            
            source_url_id = source_row[0]
            
            # Get target URL ID (create if doesn't exist)
            cursor = await conn.execute("SELECT id FROM urls WHERE url = ?", (normalized_href_url,))
            target_row = await cursor.fetchone()
            if not target_row:
                # Create the target URL if it doesn't exist
                from urllib.parse import urlparse
                parsed = urlparse(normalized_href_url)
                href_domain = parsed.netloc
                # Use the original crawl domain for classification, not the hreflang URL's domain
                crawl_domain = base_domain or href_domain
                # Classify as network since it's from sitemap hreflang data
                classification = classify_url(normalized_href_url, crawl_domain, is_from_hreflang=True)
                await conn.execute(
                    """
                    INSERT INTO urls(url, kind, classification, first_seen, last_seen)
                    VALUES (?, 'other', ?, ?, ?)
                    """,
                    (normalized_href_url, classification, int(__import__('time').time()), int(__import__('time').time()))
                )
                await conn.commit()
                
                # Get the newly created URL ID
                cursor = await conn.execute("SELECT id FROM urls WHERE url = ?", (normalized_href_url,))
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

async def batch_write_sitemaps_and_urls(sitemap_data: List[Tuple[str, List[Tuple[str, int]]]], crawl_db_path: str):
    """Write sitemap records and URL-sitemap relationships."""
    if not sitemap_data:
        return
    
    async with aiosqlite.connect(crawl_db_path) as conn:
        now = int(time.time())
        
        for sitemap_url, url_positions in sitemap_data:
            # Insert or get sitemap ID
            cursor = await conn.execute(
                "INSERT OR IGNORE INTO sitemaps (sitemap_url, discovered_at, last_crawled_at, total_urls_found) VALUES (?, ?, ?, ?)",
                (sitemap_url, now, now, len(url_positions))
            )
            
            # Get sitemap ID
            cursor = await conn.execute("SELECT id FROM sitemaps WHERE sitemap_url = ?", (sitemap_url,))
            sitemap_row = await cursor.fetchone()
            if not sitemap_row:
                continue
            sitemap_id = sitemap_row[0]
            
            # Update total URLs found
            await conn.execute(
                "UPDATE sitemaps SET total_urls_found = ?, last_crawled_at = ? WHERE id = ?",
                (len(url_positions), now, sitemap_id)
            )
            
            # Insert URL-sitemap relationships
            for url, position in url_positions:
                # Get URL ID
                cursor = await conn.execute("SELECT id FROM urls WHERE url = ?", (url,))
                url_row = await cursor.fetchone()
                if not url_row:
                    continue
                
                url_id = url_row[0]
                
                # Insert URL-sitemap relationship
                await conn.execute(
                    """
                    INSERT OR IGNORE INTO url_sitemaps(url_id, sitemap_id, position, discovered_at)
                    VALUES (?,?,?,?)
                    """,
                    (url_id, sitemap_id, position, now)
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
async def get_or_create_url_id_with_conn(url: str, base_domain: str, db_path: str, conn: aiosqlite.Connection, discovered_from: str = None, is_from_hreflang: bool = False) -> int:
    """Get URL ID, creating the URL record if it doesn't exist (with existing connection)."""
    # Try to get existing URL ID
    cursor = await conn.execute("SELECT id FROM urls WHERE url = ?", (url,))
    row = await cursor.fetchone()
    if row:
        return row[0]
    
    # Get discovered_from_id if provided
    discovered_from_id = None
    if discovered_from:
        discovered_from_id = await get_or_create_url_id_with_conn(discovered_from, base_domain, db_path, conn)
    
    # Classify the URL
    classification = classify_url(url, base_domain, is_from_hreflang=is_from_hreflang)
    
    # Create new URL record
    cursor = await conn.execute(
        "INSERT INTO urls (url, classification, discovered_from_id, first_seen, last_seen) VALUES (?, ?, ?, ?, ?)",
        (url, classification, discovered_from_id, int(time.time()), int(time.time()))
    )
    return cursor.lastrowid

# ------------------ frontier scoring ------------------

def calculate_content_type_score(url: str, content_type: str = None) -> float:
    """Calculate content type priority score for URL."""
    # Higher scores = higher priority
    
    # HTML pages get highest priority
    if content_type and 'html' in content_type.lower():
        return 1.0
    
    # Check URL patterns for content type hints
    url_lower = url.lower()
    
    # Important page types
    if any(pattern in url_lower for pattern in ['/home', '/index', '/main', '/']):
        return 1.0
    elif any(pattern in url_lower for pattern in ['/product', '/item', '/game', '/article', '/news']):
        return 0.9
    elif any(pattern in url_lower for pattern in ['/category', '/section', '/page']):
        return 0.8
    elif any(pattern in url_lower for pattern in ['/search', '/filter', '/sort']):
        return 0.6
    elif any(pattern in url_lower for pattern in ['/api', '/ajax', '/json']):
        return 0.3
    elif any(pattern in url_lower for pattern in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg']):
        return 0.2
    elif any(pattern in url_lower for pattern in ['.css', '.js', '.pdf', '.doc', '.zip']):
        return 0.1
    else:
        return 0.7  # Default for unknown content types

def calculate_depth_score(depth: int) -> float:
    """Calculate depth-based priority score."""
    # Lower depth = higher priority (closer to root)
    if depth == 0:
        return 1.0
    elif depth == 1:
        return 0.9
    elif depth == 2:
        return 0.8
    elif depth == 3:
        return 0.7
    elif depth <= 5:
        return 0.6
    else:
        return 0.5

def calculate_sitemap_priority_score(sitemap_priority: float = None) -> float:
    """Calculate sitemap priority score."""
    if sitemap_priority is None:
        return 0.5  # Default priority
    return max(0.1, min(1.0, sitemap_priority))  # Clamp between 0.1 and 1.0

def calculate_inlinks_score(inlinks_count: int) -> float:
    """Calculate inlinks-based priority score."""
    if inlinks_count == 0:
        return 0.5
    elif inlinks_count <= 5:
        return 0.6
    elif inlinks_count <= 20:
        return 0.8
    elif inlinks_count <= 100:
        return 0.9
    else:
        return 1.0

def calculate_priority_score(url: str, depth: int, sitemap_priority: float = None, 
                           inlinks_count: int = 0, content_type: str = None) -> float:
    """Calculate overall priority score for a URL."""
    # Weighted combination of different factors
    depth_score = calculate_depth_score(depth) * 0.3
    sitemap_score = calculate_sitemap_priority_score(sitemap_priority) * 0.3
    inlinks_score = calculate_inlinks_score(inlinks_count) * 0.2
    content_score = calculate_content_type_score(url, content_type) * 0.2
    
    return depth_score + sitemap_score + inlinks_score + content_score

# ------------------ frontier (pause/resume) ------------------

async def frontier_seed(start: str, base_domain: str, reset: bool = False, db_path: str = CRAWL_DB_PATH, 
                       sitemap_priority: float = None, depth: int = 0):
    now = int(time.time())
    
    # Get URL ID for start URL
    start_url_id = await get_or_create_url_id(start, base_domain, db_path)
    
    # Calculate priority score
    priority_score = calculate_priority_score(start, depth, sitemap_priority)
    content_type_score = calculate_content_type_score(start)
    
    async with aiosqlite.connect(db_path) as db:
        if reset:
            await db.execute("DELETE FROM frontier")
            # After reset, always add the start URL
            await db.execute(
                """INSERT OR IGNORE INTO frontier(url_id, depth, parent_id, status, enqueued_at, updated_at, 
                   priority_score, sitemap_priority, content_type_score) VALUES (?,?,?,?,?,?,?,?,?)""",
                (start_url_id, depth, None, 'queued', now, now, priority_score, sitemap_priority or 0.5, content_type_score),
            )
        else:
            # For non-reset calls (like sitemap URLs), always try to add
            await db.execute(
                """INSERT OR IGNORE INTO frontier(url_id, depth, parent_id, status, enqueued_at, updated_at, 
                   priority_score, sitemap_priority, content_type_score) VALUES (?,?,?,?,?,?,?,?,?)""",
                (start_url_id, depth, None, 'queued', now, now, priority_score, sitemap_priority or 0.5, content_type_score),
            )
        await db.commit()

async def frontier_next_batch(limit: int, db_path: str = CRAWL_DB_PATH) -> List[Tuple[str, int, Optional[str]]]:
    async with aiosqlite.connect(db_path) as db:
        cur = await db.execute(
            """
            SELECT f.url_id, f.depth, f.parent_id, u.url, p.url as parent_url, f.priority_score
            FROM frontier f
            JOIN urls u ON f.url_id = u.id
            LEFT JOIN urls p ON f.parent_id = p.id
            WHERE f.status='queued' 
            ORDER BY f.priority_score DESC, f.enqueued_at ASC
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
    
    # Convert URLs to IDs and calculate priority scores
    children_with_scores = []
    for (url, depth, parent_url) in children:
        url_id = await get_or_create_url_id(url, base_domain, db_path)
        parent_id = await get_or_create_url_id(parent_url, base_domain, db_path) if parent_url else None
        
        # Calculate priority score for this URL
        priority_score = calculate_priority_score(url, depth)
        content_type_score = calculate_content_type_score(url)
        
        children_with_scores.append((url_id, depth, parent_id, priority_score, content_type_score))
    
    async with aiosqlite.connect(db_path) as db:
        await db.executemany(
            """
        INSERT OR IGNORE INTO frontier(url_id, depth, parent_id, status, enqueued_at, updated_at, 
                                      priority_score, sitemap_priority, content_type_score)
        VALUES (?,?,?,?,?,?,?,?,?)
        """,
            [(url_id, d, p_id, 'queued', now, now, priority_score, 0.5, content_type_score) 
             for (url_id, d, p_id, priority_score, content_type_score) in children_with_scores],
        )
        await db.commit()

async def frontier_update_priority_scores(db_path: str = CRAWL_DB_PATH):
    """Update priority scores for all queued URLs based on current inlinks count."""
    async with aiosqlite.connect(db_path) as db:
        # Get inlinks count for each URL
        cur = await db.execute("""
            SELECT f.url_id, f.depth, f.sitemap_priority, f.content_type_score, u.url,
                   COALESCE(COUNT(il.target_url_id), 0) as inlinks_count
            FROM frontier f
            JOIN urls u ON f.url_id = u.id
            LEFT JOIN internal_links il ON f.url_id = il.target_url_id
            WHERE f.status = 'queued'
            GROUP BY f.url_id, f.depth, f.sitemap_priority, f.content_type_score, u.url
        """)
        rows = await cur.fetchall()
        
        # Update priority scores
        for row in rows:
            url_id, depth, sitemap_priority, content_type_score, url, inlinks_count = row
            priority_score = calculate_priority_score(url, depth, sitemap_priority, inlinks_count)
            
            await db.execute("""
                UPDATE frontier 
                SET priority_score = ?, inlinks_count = ?
                WHERE url_id = ?
            """, (priority_score, inlinks_count, url_id))
        
        await db.commit()

async def frontier_scoring_stats(db_path: str = CRAWL_DB_PATH) -> Dict:
    """Return frontier scoring statistics."""
    async with aiosqlite.connect(db_path) as db:
        cur = await db.execute("""
            SELECT 
                AVG(priority_score) as avg_priority,
                MAX(priority_score) as max_priority,
                MIN(priority_score) as min_priority,
                COUNT(CASE WHEN sitemap_priority > 0.5 THEN 1 END) as sitemap_priority_count,
                AVG(inlinks_count) as avg_inlinks
            FROM frontier 
            WHERE status = 'queued'
        """)
        row = await cur.fetchone()
        if row and row[0] is not None:
            return {
                'avg_priority': row[0],
                'max_priority': row[1],
                'min_priority': row[2],
                'sitemap_priority_count': row[3],
                'avg_inlinks': row[4]
            }
        return None

async def frontier_stats(db_path: str = CRAWL_DB_PATH) -> Tuple[int, int]:
    """Return (#queued, #done)."""
    async with aiosqlite.connect(db_path) as db:
        cur = await db.execute("SELECT SUM(status='queued'), SUM(status='done') FROM frontier")
        row = await cur.fetchone()
        return (int(row[0] or 0), int(row[1] or 0))


# ------------------ Schema.org functions ------------------

async def get_or_create_schema_instance(schema_data: Dict[str, Any], conn: aiosqlite.Connection, db_path: str = CRAWL_DB_PATH) -> int:
    """Get or create a schema instance and return its ID."""
    content_hash = schema_data.get('content_hash', '')
    if not content_hash:
        # If no hash provided, create one
        from .schema import create_schema_content_hash
        parsed_data_str = schema_data.get('parsed_data', '{}')
        if parsed_data_str is None:
            parsed_data_str = '{}'
        parsed_data = json.loads(parsed_data_str)
        content_hash = create_schema_content_hash(parsed_data)
    
    if conn:
        # Use existing connection
        cur = await conn.execute(
            "SELECT id FROM schema_instances WHERE content_hash = ?",
            (content_hash,)
        )
        existing = await cur.fetchone()
        
        if existing:
            return existing[0]
        
        # Create new instance
        schema_type_id = await get_or_create_schema_type_id(db_path, schema_data['type'], conn)
        
        # Default format if not specified
        format_type = schema_data.get('format', 'json-ld')
        if format_type not in ['json-ld', 'microdata', 'rdfa']:
            format_type = 'json-ld'  # Default to json-ld
        
        await conn.execute("""
            INSERT INTO schema_instances 
            (content_hash, schema_type_id, format, raw_data, parsed_data, is_valid, validation_errors, severity, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            content_hash,
            schema_type_id,
            format_type,
            schema_data.get('raw_data', ''),
            schema_data.get('parsed_data'),
            schema_data.get('is_valid', True),
            json.dumps(schema_data.get('validation_errors', [])),
            schema_data.get('severity', 'info'),
            int(time.time())
        ))
        
        # Get the new instance ID
        cur = await conn.execute(
            "SELECT id FROM schema_instances WHERE content_hash = ?",
            (content_hash,)
        )
        result = await cur.fetchone()
        return result[0]
    else:
        # Create new connection
        async with aiosqlite.connect(db_path) as db:
            # Check if instance already exists
            cur = await db.execute(
                "SELECT id FROM schema_instances WHERE content_hash = ?",
                (content_hash,)
            )
            existing = await cur.fetchone()
            
            if existing:
                return existing[0]
            
            # Create new instance
            schema_type_id = await get_or_create_schema_type_id(db_path, schema_data['type'], db)
            
            await db.execute("""
                INSERT INTO schema_instances 
                (content_hash, schema_type_id, format, raw_data, parsed_data, is_valid, validation_errors, severity, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                content_hash,
                schema_type_id,
                schema_data['format'],
                schema_data['raw_data'],
                schema_data.get('parsed_data'),
                schema_data.get('is_valid', True),
                json.dumps(schema_data.get('validation_errors', [])),
                schema_data.get('severity', 'info'),
                int(time.time())
            ))
            
            await db.commit()
            
            # Get the new instance ID
            cur = await db.execute(
                "SELECT id FROM schema_instances WHERE content_hash = ?",
                (content_hash,)
            )
            result = await cur.fetchone()
            return result[0]


async def create_page_schema_references_with_conn(url_id: int, schema_items: List[Dict[str, Any]], conn: aiosqlite.Connection, crawl_db_path: str) -> None:
    """Create page schema references with hierarchical relationships using existing connection."""
    from .schema import identify_schema_relationships
    
    # Identify relationships
    relationships = identify_schema_relationships(schema_items)
    main_entity = relationships['main_entity']
    properties = relationships['properties']
    related_entities = relationships['related_entities']
    
    now = int(time.time())
    
    # Create reference for main entity
    if main_entity:
        main_instance_id = await get_or_create_schema_instance(main_entity, conn, crawl_db_path)
        await conn.execute("""
            INSERT INTO page_schema_references 
            (url_id, schema_instance_id, position, is_main_entity, discovered_at)
            VALUES (?, ?, ?, ?, ?)
        """, (url_id, main_instance_id, main_entity.get('position', 0), True, now))
        
        # Get the last inserted row ID
        cursor = await conn.execute("SELECT last_insert_rowid()")
        main_ref_id = (await cursor.fetchone())[0]
        
        # Create references for properties (linked to main entity)
        for prop in properties:
            prop_instance_id = await get_or_create_schema_instance(prop, conn, crawl_db_path)
            await conn.execute("""
                INSERT INTO page_schema_references 
                (url_id, schema_instance_id, position, property_name, is_main_entity, parent_entity_id, discovered_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                url_id, 
                prop_instance_id, 
                prop.get('position', 0), 
                prop.get('type', '').lower(), 
                False, 
                main_ref_id, 
                now
            ))
    
    # Create references for related entities (standalone)
    for entity in related_entities:
        entity_instance_id = await get_or_create_schema_instance(entity, conn, crawl_db_path)
        await conn.execute("""
            INSERT INTO page_schema_references 
            (url_id, schema_instance_id, position, is_main_entity, discovered_at)
            VALUES (?, ?, ?, ?, ?)
        """, (url_id, entity_instance_id, entity.get('position', 0), False, now))


async def create_page_schema_references(url_id: int, schema_items: List[Dict[str, Any]], db_path: str = CRAWL_DB_PATH) -> None:
    """Create page schema references with hierarchical relationships."""
    from .schema import identify_schema_relationships
    
    # Identify relationships
    relationships = identify_schema_relationships(schema_items)
    main_entity = relationships['main_entity']
    properties = relationships['properties']
    related_entities = relationships['related_entities']
    
    async with aiosqlite.connect(db_path) as db:
        now = int(time.time())
        
        # Create reference for main entity
        if main_entity:
            main_instance_id = await get_or_create_schema_instance(main_entity, db_path, db)
            await db.execute("""
                INSERT INTO page_schema_references 
                (url_id, schema_instance_id, position, is_main_entity, discovered_at)
                VALUES (?, ?, ?, ?, ?)
            """, (url_id, main_instance_id, main_entity.get('position', 0), True, now))
            
            main_ref_id = db.lastrowid
            
            # Create references for properties (linked to main entity)
            for prop in properties:
                prop_instance_id = await get_or_create_schema_instance(prop, db_path, db)
                await db.execute("""
                    INSERT INTO page_schema_references 
                    (url_id, schema_instance_id, position, property_name, is_main_entity, parent_entity_id, discovered_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    url_id, 
                    prop_instance_id, 
                    prop.get('position', 0), 
                    prop.get('type', '').lower(), 
                    False, 
                    main_ref_id, 
                    now
                ))
        
        # Create references for related entities (standalone)
        for entity in related_entities:
            entity_instance_id = await get_or_create_schema_instance(entity, db_path, db)
            await db.execute("""
                INSERT INTO page_schema_references 
                (url_id, schema_instance_id, position, is_main_entity, discovered_at)
                VALUES (?, ?, ?, ?, ?)
            """, (url_id, entity_instance_id, entity.get('position', 0), False, now))
        
        await db.commit()


async def get_or_create_schema_type_id(crawl_db_path: str, type_name: str, conn: aiosqlite.Connection = None) -> int:
    """Get or create a schema type ID."""
    if conn:
        # Use existing connection
        cursor = await conn.execute("SELECT id FROM schema_types WHERE type_name = ?", (type_name,))
        result = await cursor.fetchone()
        
        if result:
            return result[0]
        
        # Create new schema type
        cursor = await conn.execute("INSERT INTO schema_types (type_name) VALUES (?)", (type_name,))
        return cursor.lastrowid
    else:
        # Create new connection with timeout and retry
        for attempt in range(3):
            try:
                async with aiosqlite.connect(crawl_db_path, timeout=30.0) as db:
                    # Try to get existing ID
                    cursor = await db.execute("SELECT id FROM schema_types WHERE type_name = ?", (type_name,))
                    result = await cursor.fetchone()
                    
                    if result:
                        return result[0]
                    
                    # Create new schema type
                    cursor = await db.execute("INSERT INTO schema_types (type_name) VALUES (?)", (type_name,))
                    await db.commit()
                    return cursor.lastrowid
            except aiosqlite.OperationalError as e:
                if "database is locked" in str(e) and attempt < 2:
                    import asyncio
                    await asyncio.sleep(0.1 * (attempt + 1))  # Exponential backoff
                    continue
                raise


async def batch_write_schema_data(schema_data_list: List[Dict[str, Any]], crawl_db_path: str):
    """Write schema data to database in batch using normalized structure."""
    if not schema_data_list:
        return
    
    # Group schema data by URL
    url_schemas = {}
    for item in schema_data_list:
        url = item['url']
        if url not in url_schemas:
            url_schemas[url] = []
        url_schemas[url].append(item)
    
    # Process each URL's schema data
    for url, schema_items in url_schemas.items():
        # Get URL ID
        async with aiosqlite.connect(crawl_db_path) as db:
            cursor = await db.execute("SELECT id FROM urls WHERE url = ?", (url,))
            result = await cursor.fetchone()
            if result:
                url_id = result[0]
                # Use the new normalized schema storage
                await create_page_schema_references(url_id, schema_items, crawl_db_path)