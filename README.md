# SQLiteCrawler

A high-performance, persistent web crawler built with Python and SQLite. Features intelligent URL discovery, redirect tracking, content extraction, and comprehensive data storage with optimized performance settings.

## Performance

SQLiteCrawler uses optimized settings for maximum performance:
- **Parallel database operations** for faster I/O
- **Optimized batch sizes** (100/500/1000/100) for better throughput
- **Intelligent retry logic** with exponential backoff
- **7.1% faster** than baseline configuration

See [PERFORMANCE_ANALYSIS.md](PERFORMANCE_ANALYSIS.md) for detailed benchmark results.

## Features

- **Persistent Frontier**: Resume crawls from where you left off
- **Redirect Tracking**: Complete redirect chain capture and storage
- **Content Extraction**: Titles, meta descriptions, H1/H2 tags, robots directives, canonicals
- **Sitemap Discovery**: Automatic XML sitemap parsing and URL discovery
- **Robots.txt Compliance**: Respects crawling policies, discovers sitemaps, and analyzes crawlability
- **Link Analysis**: Internal/external link tracking with anchor text, XPath, and metadata
- **Schema.org Extraction**: Extracts and validates JSON-LD, microdata, and RDFa structured data with normalized storage and hierarchical relationships
- **Hreflang Support**: Extracts and normalizes hreflang data from sitemaps
- **CSV Crawl Support**: Crawl from predefined URL lists with restricted or seed modes
- **HTTP/2 & Brotli Support**: Modern HTTP/2 client with Brotli compression for improved performance
- **Intelligent Frontier Scoring**: Prioritizes URLs by depth, sitemap priority, inlinks, and content type for optimal crawl efficiency
- **Database Normalization**: Efficient storage with URL IDs and compressed content
- **Async Performance**: Concurrent requests with configurable limits
- **Flexible Configuration**: Multiple user agents, timeout settings, and crawl limits

## Installation

```bash
# Clone the repository
git clone https://github.com/user256/SQLiteCrawler.git
cd SQLiteCrawler

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -e .

# Optional: Install JavaScript rendering support
pip install -e .[js]
playwright install
```

## Quick Start

### Basic Crawling

```bash
# Crawl a website with default settings (500 pages, depth 3, internal only)
python main.py https://example.com/

# Crawl with custom limits
python main.py https://example.com/ --max-pages 1000 --max-depth 2

# Allow crawling external URLs
python main.py https://example.com/ --allow-external

# Resume a previous crawl (no reset needed)
python main.py https://example.com/ --max-pages 200

# Crawl with HTTP authentication (for staging sites)
python main.py https://staging.example.com/ --auth-username myuser --auth-password mypass

# Bearer token authentication
python main.py https://api.example.com/ --auth-type bearer --auth-token "your-bearer-token"

# JWT token authentication
python main.py https://api.example.com/ --auth-type jwt --auth-token "your-jwt-token"

# API key authentication
python main.py https://api.example.com/ --auth-type api_key --auth-token "your-api-key"

# Custom API key header
python main.py https://api.example.com/ --auth-type api_key --auth-token "your-key" --auth-header "X-API-Key"

# Custom headers
python main.py https://api.example.com/ --auth-type custom --auth-custom-headers "X-Custom-Header:Value1,X-Another-Header:Value2"

# Disable HTTP/2 (use HTTP/1.1)
python main.py https://example.com/ --no-http2

# Disable Brotli compression
python main.py https://example.com/ --no-brotli

# Configure adaptive delay settings
python main.py https://example.com/ --min-delay 0.5 --max-delay 5.0 --delay-increase 2.0

# Disable adaptive delay (use fixed delay only)
python main.py https://example.com/ --no-adaptive-delay
```

### Authentication Methods

SQLiteCrawler supports multiple authentication methods for protected sites and APIs:

**Basic/Digest Authentication:**
```bash
python main.py https://staging.example.com/ --auth-username myuser --auth-password mypass --auth-type basic
```

**Bearer Token Authentication:**
```bash
python main.py https://api.example.com/ --auth-type bearer --auth-token "your-bearer-token"
```

**JWT Token Authentication:**
```bash
python main.py https://api.example.com/ --auth-type jwt --auth-token "your-jwt-token"
```

**API Key Authentication:**
```bash
python main.py https://api.example.com/ --auth-type api_key --auth-token "your-api-key"
python main.py https://api.example.com/ --auth-type api_key --auth-token "your-key" --auth-header "X-API-Key"
```

**Custom Headers:**
```bash
python main.py https://api.example.com/ --auth-type custom --auth-custom-headers "X-Custom-Header:Value1,X-Another-Header:Value2"
```

**Domain-Restricted Authentication:**
```bash
python main.py https://example.com/ --auth-username myuser --auth-password mypass --auth-domain "staging.example.com"
```

### Adaptive Delay & Politeness

SQLiteCrawler implements intelligent adaptive delay to be respectful to target servers:

**Automatic Rate Limiting:**
- Starts with a configurable base delay (default: 0.2s)
- Automatically increases delay when receiving 429 (rate limited) or 5xx errors
- Gradually decreases delay on successful responses
- Tracks delays per host independently

**Configuration Options:**
```bash
# Set minimum and maximum delays
python main.py https://example.com/ --min-delay 0.1 --max-delay 10.0

# Adjust backoff factors
python main.py https://example.com/ --delay-increase 1.5 --delay-decrease 0.9

# Disable adaptive delay (use fixed delay only)
python main.py https://example.com/ --no-adaptive-delay
```

**Response-Based Adjustments:**
- **429/503/502/504**: Increase delay by 2x the increase factor
- **408/423/420/451**: Increase delay by the increase factor  
- **200/304 responses**: Gradually decrease delay back to base rate
- **Per-host tracking**: Each domain has independent delay settings

**Robots.txt Crawl-Delay Integration:**
- **Automatic extraction**: Parses `Crawl-delay` directives from robots.txt
- **User-agent specific**: Respects per-user-agent crawl-delay settings
- **Intelligent combination**: Uses maximum of robots.txt delay and adaptive delay
- **TTL caching**: Caches robots.txt files for 24 hours to avoid repeated requests
- **Verbose reporting**: Shows robots.txt crawl-delay in delay statistics

### Intelligent Frontier Scoring

SQLiteCrawler uses intelligent URL prioritization to crawl the most important pages first:

**Multi-Factor Scoring System:**
- **Depth-based scoring**: Prioritizes pages closer to the root (depth 0 = highest priority)
- **Sitemap priority**: Respects XML sitemap priority values when available
- **Inlinks count**: Pages with more internal links get higher priority
- **Content type scoring**: HTML pages prioritized over assets (images, CSS, JS)
- **URL pattern analysis**: Recognizes important page types (/home, /product, /article, etc.)

**Automatic Priority Updates:**
- Priority scores recalculated every 50 pages based on discovered inlinks
- Real-time frontier reordering ensures optimal crawl progression
- Detailed scoring statistics reported in verbose mode

### Normalized Schema Storage

SQLiteCrawler implements intelligent schema storage with significant space savings:

**Content-Based Deduplication:**
- **SHA256 hashing**: Identical schema instances stored only once
- **Normalized content**: Removes variable fields (@id, timestamps, URLs) for consistent hashing
- **Storage efficiency**: 30-90% reduction in schema storage depending on site structure

**Hierarchical Relationships:**
- **Main entity identification**: Automatically identifies primary schema entities (WebPage, Article, Product)
- **Property relationships**: Links nested properties (ImageObject, BreadcrumbList) to main entities
- **Parent-child structure**: Maintains schema hierarchy instead of flat storage

**Example Storage Savings:**
- **Organization schema**: Used on 10 pages, stored once with 10 references (90% savings)
- **WebSite schema**: Used on 9 pages, stored once with 9 references (89% savings)
- **Person schema**: Used on 4 pages, stored once with 4 references (75% savings)

### Conditional Requests & Efficiency

SQLiteCrawler implements HTTP conditional requests to avoid re-downloading unchanged content:

**Automatic ETag & Last-Modified Support:**
- Stores ETag and Last-Modified headers from previous responses
- Automatically sends `If-None-Match` and `If-Modified-Since` headers on re-crawls
- Handles 304 Not Modified responses efficiently (skips content processing)
- Reduces bandwidth usage and improves crawl speed

**Configuration Options:**
```bash
# Disable conditional requests (download all content)
python main.py https://example.com/ --no-conditional-requests
```

**Benefits:**
- **Bandwidth savings**: Skip unchanged content
- **Faster crawls**: Reduce processing time for unchanged pages
- **Server-friendly**: Respects HTTP caching semantics
- **Automatic**: No configuration required, works out of the box

### CSV Crawl Support

Crawl from a predefined list of URLs in CSV format:

```bash
# CSV restricted mode - only crawl URLs from the CSV file
python main.py --csv-file urls.csv --csv-column url

# CSV seed mode - use CSV URLs as starting points, then discover sitemaps and follow internal links
python main.py --csv-file urls.csv --csv-column url --csv-seed

# CSV with custom column name
python main.py --csv-file urls.csv --csv-column website_url --csv-seed
```

**CSV Format Examples:**

```csv
url,priority
https://example.com/,high
https://example.com/about/,medium
https://example.com/contact/,low
```

Or simple list format (one URL per line):
```
https://example.com/
https://example.com/about/
https://example.com/contact/
```

### Single URL Crawling

```bash
# Crawl only a specific URL without sitemap discovery
python main.py https://example.com/specific-page --skip-sitemaps --max-pages 1

# Test redirect tracking on a specific URL
python main.py https://example.com/redirecting-page --skip-sitemaps --max-pages 1 --verbose
```

### Advanced Configuration

```bash
# Custom user agent
python main.py https://example.com/ --user-agent screaming-frog

# Custom user agent string
python main.py https://example.com/ --custom-ua "MyBot/1.0"

# Adjust performance settings
python main.py https://example.com/ --concurrency 20 --timeout 30 --delay 0.5

# Skip robots.txt compliance (still discovers sitemaps)
python main.py https://example.com/ --ignore-robots

# Skip sitemap discovery entirely
python main.py https://example.com/ --skip-sitemaps

# Skip robots.txt sitemap parsing (use common locations only)
python main.py https://example.com/ --skip-robots-sitemaps
```

### JavaScript Rendering

```bash
# Enable JavaScript rendering for SPAs
python main.py https://example.com/ --js
```

## Database Schema

### Pages Database (`*_pages.db`)
- **Raw HTML storage** (compressed with zlib/base64)
- **HTTP headers** (compressed)
- **Status codes and timestamps**

### Crawl Database (`*_crawl.db`)
- **`urls`**: Normalized URL list with classifications (internal/external/social/network)
- **`frontier`**: Persistent crawl queue with depth tracking
- **`content`**: Extracted content (titles, meta descriptions, H1/H2, robots, canonicals, link counts)
- **`internal_links`**: Normalized internal link tracking with anchor text, XPath, and href metadata
- **`redirects`**: Complete redirect chains with source/target URLs
- **`indexability`**: Robots.txt, HTML meta, and HTTP header analysis for crawlability
- **`hreflang_*`**: Normalized hreflang data from sitemaps, HTTP headers, and HTML
- **`sitemaps_listed`**: URLs discovered from sitemaps for validation
- **`schema_data`**: Schema.org structured data (JSON-LD, microdata, RDFa) with validation
- **`schema_types`**: Normalized schema.org type names

## Command Line Options

### Crawl Limits
- `--max-pages N`: Maximum pages to crawl (default: no limit)
- `--max-depth N`: Maximum crawl depth (default: 3)
- `--offsite`: Allow crawling external URLs (default: internal only)

### HTTP Configuration
- `--user-agent {screaming-frog,paradise-crawler,googlebot,custom}`: Predefined user agents
- `--custom-ua STRING`: Custom user agent string
- `--timeout N`: Request timeout in seconds (default: 20)
- `--concurrency N`: Maximum concurrent requests (default: 10)
- `--delay N`: Delay between requests in seconds (default: 0.1)

### Authentication
- `--auth-username STRING`: Username for HTTP authentication (basic/digest)
- `--auth-password STRING`: Password for HTTP authentication (basic/digest)
- `--auth-type {basic,digest}`: Authentication type (default: basic)
- `--auth-domain STRING`: Restrict authentication to specific domain (optional)

### Robots and Sitemaps
- `--ignore-robots`: Ignore robots.txt for crawlability (still use for sitemaps)
- `--skip-robots-sitemaps`: Skip robots.txt sitemap discovery
- `--skip-sitemaps`: Skip all sitemap discovery and processing

### Performance
- `--max-workers N`: Maximum worker threads for database operations (default: 2)
- `--js`: Enable JavaScript rendering with Playwright

### Output
- `--verbose, -v`: Enable verbose output
- `--quiet, -q`: Suppress non-error output
- `--reset-frontier`: Clear existing crawl state

## Examples

### SEO Audit
```bash
# Comprehensive crawl with content extraction
python main.py https://example.com/ --max-pages 5000 --verbose --user-agent screaming-frog
```

### Redirect Analysis
```bash
# Test specific redirecting URLs
python main.py https://example.com/old-page --skip-sitemaps --max-pages 1 --verbose
```

### Sitemap Validation
```bash
# Discover and validate sitemap URLs
python main.py https://example.com/ --max-pages 100 --verbose
```

### Staging Site Crawling
```bash
# Crawl staging site with HTTP authentication
python main.py https://staging.example.com/ --auth-username deploy --auth-password secret123 --verbose

# Crawl with domain-restricted authentication
python main.py https://staging.example.com/ --auth-username user --auth-password pass --auth-domain staging.example.com
```

### Performance Testing
```bash
# High-performance crawl
python main.py https://example.com/ --concurrency 50 --timeout 10 --max-pages 10000
```

## Environment Variables

Override default settings with environment variables:

```bash
export SQLITECRAWLER_MAX_PAGES=1000
export SQLITECRAWLER_MAX_DEPTH=5
export SQLITECRAWLER_SAME_HOST_ONLY=0
export SQLITECRAWLER_UA="MyBot/1.0"
export SQLITECRAWLER_TIMEOUT=30
export SQLITECRAWLER_CONCURRENCY=20
export SQLITECRAWLER_DELAY=0.2
export SQLITECRAWLER_RESPECT_ROBOTS=0
```

## Database Queries

### View Crawled Pages
```sql
SELECT url, status_code, title, meta_description, h1_count, h2_count 
FROM page_analysis 
WHERE status_code = 200 
ORDER BY discovered_at DESC;
```

### Check Redirects
```sql
SELECT u1.url as source, u2.url as target, chain_length, final_status
FROM redirects r
JOIN urls u1 ON r.source_url_id = u1.id
JOIN urls u2 ON r.target_url_id = u2.id;
```

### Analyze Hreflang Data
```sql
SELECT u.url, h.hreflang, href_urls.url as target_url
FROM hreflang_sitemap hs
JOIN urls u ON hs.url_id = u.id
JOIN hreflang_languages h ON hs.hreflang_id = h.id
JOIN urls href_urls ON hs.href_url_id = href_urls.id;
```

### Check Robots.txt Analysis
```sql
SELECT u.url, i.robots_txt_allows, i.html_meta_allows, i.overall_indexable
FROM indexability i
JOIN urls u ON i.url_id = u.id
WHERE i.robots_txt_allows = 0;  -- Find disallowed URLs
```

### Analyze Internal Links
```sql
SELECT source_url, target_url, anchor_text, xpath, href
FROM internal_links_analysis
WHERE source_url LIKE '%example.com%';
```

### Analyze Schema.org Data
```sql
-- Find all pages with structured data
SELECT url, schema_type, format, is_valid, validation_errors
FROM schema_analysis
WHERE is_valid = 1;

-- Count schema types by format
SELECT schema_type, format, COUNT(*) as count
FROM schema_analysis
GROUP BY schema_type, format
ORDER BY count DESC;

-- Find pages with invalid schema
SELECT url, schema_type, validation_errors
FROM schema_analysis
WHERE is_valid = 0;
```

## Performance Tips

1. **Use appropriate concurrency**: Start with 10, increase based on server response
2. **Set reasonable delays**: 0.1s default, increase for slower servers
3. **Monitor memory usage**: Large crawls may need database optimization
4. **Use `--skip-sitemaps`** for single URL testing
5. **Enable verbose mode** for debugging and monitoring

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Changelog

### v0.3
- **Fixed robots.txt analysis**: Proper parsing and integration of robots.txt compliance
- **Enhanced link analysis**: Internal/external link tracking with anchor text, XPath, and metadata
- **Database normalization**: Fully normalized tables for anchor texts, xpaths, hrefs, canonical URLs, and robots directives
- **Improved crawl limits**: Default to no page limit for complete site crawling
- **Added sitemap tracking**: Track URLs discovered from sitemaps for validation
- **Enhanced indexability analysis**: Comprehensive robots.txt, HTML meta, and HTTP header analysis
- **Performance optimizations**: Reduced default concurrency and workers for better stability

### v0.2
- Added redirect tracking with complete chain capture
- Implemented content extraction (titles, meta, H1/H2, robots, canonicals)
- Added sitemap discovery and hreflang extraction
- Database normalization with URL IDs
- Performance improvements with async operations
- Added `--skip-sitemaps` and `--skip-robots-sitemaps` flags
- Enhanced user agent options and configuration

### v0.1
- Basic persistent crawling with SQLite storage
- Frontier management with depth tracking
- Robots.txt compliance
- Configurable limits and settings