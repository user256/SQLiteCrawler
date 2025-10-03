# SQLiteCrawler

A high-performance, persistent web crawler built with Python and SQLite. Features intelligent URL discovery, redirect tracking, content extraction, and comprehensive data storage.

## Features

- **Persistent Frontier**: Resume crawls from where you left off
- **Redirect Tracking**: Complete redirect chain capture and storage
- **Content Extraction**: Titles, meta descriptions, H1/H2 tags, robots directives, canonicals
- **Sitemap Discovery**: Automatic XML sitemap parsing and URL discovery
- **Robots.txt Compliance**: Respects crawling policies, discovers sitemaps, and analyzes crawlability
- **Link Analysis**: Internal/external link tracking with anchor text, XPath, and metadata
- **Hreflang Support**: Extracts and normalizes hreflang data from sitemaps
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