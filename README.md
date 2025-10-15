# SQLiteCrawler

A web crawler for those compfortable with Python and SQLite. Features URL discovery, redirect tracking, content extraction and comparison, including across sites. 

## 🚀 Quick Start

```bash
# Basic crawl
python main.py https://example.com

# Crawl with JavaScript rendering
python main.py https://example.com --js

# Crawl with custom limits
python main.py https://example.com --max-pages 100 --max-depth 3

# Crawl comparison (origin vs staging)
python main.py https://example.com --compare-domain https://staging.example.com
```

## History
SQLiteCrawler started as a collection of ad-hoc scripts under the equally cleverly named SeoToolz but branched out to be a more fully formed crawler trying to solve a couple of personal pain points I have with the otherwise exceptional Screaming Frog
 - **a queryable DB**: when I crawl the web I often want to get very specific information out of screaming frog that involves slow exports and a then excel lookups (or having to push them into a db and doing lot of joins)
 - **retry functionality**: Sometimes webpages aren't available on the first pass, pages time out or because we're getting blocked. The ability to retry failed URLs removes the need for manual intervention 
 - **The ability to compare across doamins**: SF does a solid job of comparing crawls of a single domain but is a lot less useful when you need to compare against staging sites, in particular when those sites don't entirely match up. SQLiteCrawler follows redirects on a staging site allowing you to compare the content on the origin, against the final destination on staging.  
 - **List restricted crawling**: A personal pet peeve is the need to untick a large number of boxes to limit a crawl to only a list of provided URLs 

## ✨ Key Features

### **Core Crawling**
- **Persistent Frontier**: Resume crawls from where you left off
- **Redirect Tracking**: Complete redirect chain capture and storage
- **Content Extraction**: Titles, meta descriptions, H1/H2 tags, robots directives, canonicals
- **Sitemap Discovery**: Automatic XML sitemap parsing and URL discovery
- **Robots.txt Compliance**: Respects crawling policies and analyzes crawlability

### **Advanced Analysis**
- **Link Analysis**: Internal/external link tracking with anchor text, XPath, and metadata
- **Schema.org Extraction**: Extracts and validates JSON-LD, microdata, and RDFa structured data
- **Hreflang Support**: Extracts and normalizes hreflang data from sitemaps
- **CSV Crawl Support**: Crawl from predefined URL lists with restricted or seed modes
- **Content Hashing**: SHA256 and SimHash for duplicate detection and content comparison

### **Crawl Comparison**
- **Origin vs Staging**: Compare production and staging environments
- **Content Analysis**: Track title, H1, meta description, and word count changes
- **URL Move Detection**: Identify content moved via 301 redirects
- **Comprehensive Views**: Detailed analysis of differences and issues

### **Performance & Reliability**
- **HTTP/2 & Brotli Support**: Modern HTTP/2 client with Brotli compression
- **Intelligent Frontier Scoring**: Prioritizes URLs by depth, sitemap priority, and inlinks
- **Database Normalization**: Efficient storage with URL IDs and compressed content
- **Async Performance**: Concurrent requests with configurable limits

## 🛠️ Installation

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

## 📖 Usage Examples

### **Basic Crawling**

```bash
# Simple crawl
python main.py https://example.com

# Crawl with JavaScript rendering
python main.py https://example.com --js

# Crawl with custom limits
python main.py https://example.com --max-pages 100 --max-depth 3

# Crawl with custom concurrency
python main.py https://example.com --concurrency 20 --delay 0.5
```

### **Authentication**

```bash
# Basic authentication
python main.py https://example.com --auth-username user --auth-password pass --auth-type basic

# Digest authentication
python main.py https://example.com --auth-username user --auth-password pass --auth-type digest

# Bearer token
python main.py https://example.com --auth-token your-token --auth-type bearer
```

### **CSV Crawl Support**

```bash
# Crawl specific URLs from CSV
python main.py --csv-file urls.csv --csv-column url

# Use CSV as seed URLs (also crawl sitemaps and follow links)
python main.py --csv-file urls.csv --csv-seed
```

### **Crawl Comparison**

```bash
# Basic comparison
python main.py https://example.com --compare-domain https://staging.example.com

# With commercial pages analysis
python main.py https://example.com --compare-domain https://staging.example.com --commercial-csv commercial.csv

# With detailed link comparison
python main.py https://example.com --compare-domain https://staging.example.com --compare-links

# With authentication for staging
python main.py https://example.com --compare-domain https://staging.example.com --auth-username user --auth-password pass
```

**Comparison Features:**
- **Automatic seed list generation** from origin crawl
- **Comprehensive analysis views** for missing/new URLs
- **Content comparison** (titles, H1s, meta descriptions, word counts)
- **URL move detection** (301 redirects, moved content tracking)
- **Schema markup comparison** (JSON-LD, Microdata, RDFa)
- **Optional commercial pages analysis** with CSV input
- **Optional link comparison** (added/lost internal links)

## 🗄️ Database Schema

### **Pages Database (`*_pages.db`)**
- `pages`: Raw HTML content and HTTP headers
- `content`: Extracted content (titles, meta descriptions, H1/H2 tags, word counts)
- `schema_data`: Structured data (JSON-LD, microdata, RDFa)

### **Crawl Database (`*_crawl.db`)**
- `urls`: Discovered URLs with classification (internal/external/network/social/subdomain)
- `frontier`: Crawl queue with priority scoring
- `internal_links`: Link relationships with anchor text and XPath
- `redirects`: Redirect chains and final destinations
- `hreflang_sitemap`: Hreflang data from sitemaps
- `robots_txt`: Robots.txt analysis and directives
- `fragments`: URL fragments for better normalization

### **Comparison Database (`*_vs_*_comparison.db`)**
- `comparison_urls`: URL mapping with content analysis and move tracking
- `comparison_sessions`: Comparison run metadata
- `commercial_pages`: Commercial page analysis (optional)

## 📊 Analysis Views

### **Standard Views**
- `view_crawl_overview`: Comprehensive crawl summary
- `view_links_internal`: Internal link analysis
- `view_links_network`: Network link analysis  
- `view_links_external`: External link analysis
- `view_links_subdomain`: Subdomain link analysis
- `view_schema_data`: Structured data analysis
- `view_hubs`: Pages with multiple child pages

### **Comparison Views**
- `view_sitemap_changes`: URLs in origin vs staging sitemaps
- `view_urls_missing`: URLs on origin not found on staging
- `view_urls_new`: URLs on staging not found on origin
- `view_content_differences`: Detailed content comparison
- `view_url_moves`: URLs moved via redirects
- `view_crawl_overview_comparison`: Comprehensive comparison summary

## ⚙️ Configuration Options

### **Crawl Limits**
- `--max-pages`: Maximum pages to crawl (default: unlimited)
- `--max-depth`: Maximum crawl depth (default: 3)
- `--offsite`: Allow offsite traversal (default: same host only)

### **HTTP Configuration**
- `--concurrency`: Maximum concurrent requests (default: 10)
- `--delay`: Delay between requests in seconds (default: 0.1)
- `--timeout`: Request timeout in seconds (default: 20)
- `--user-agent`: User agent type (default, chrome, firefox, safari, edge, mobile, random)

### **Advanced Options**
- `--js`: Enable JavaScript rendering via Playwright
- `--ignore-robots`: Ignore robots.txt for crawlability
- `--skip-sitemaps`: Skip sitemap discovery
- `--reset-frontier`: Clear and reseed the frontier

## 🔍 Example Queries

### **Basic Analysis**
```sql
-- View crawled pages
SELECT url, title, h1_1, word_count, status_code 
FROM view_crawl_overview 
WHERE status_code = 200 
ORDER BY word_count DESC;

-- Check redirects
SELECT source_url, redirect_destination_url, chain_length 
FROM redirects 
WHERE chain_length > 1;

-- Analyze internal links
SELECT source_url, target_url, anchor_text, is_image 
FROM view_links_internal 
WHERE is_image = 1;

-- Find content duplicates
SELECT url, content_hash_sha256, COUNT(*) as duplicate_count
FROM content 
WHERE content_hash_sha256 IS NOT NULL
GROUP BY content_hash_sha256 
HAVING COUNT(*) > 1;
```

### **Comparison Analysis**
```sql
-- View content differences
SELECT path, origin_title, staging_title, title_match, 
       origin_word_count, staging_word_count, word_count_match
FROM view_content_differences 
WHERE overall_content_status = 'Content differences detected';

-- Check URL moves
SELECT path, moved_from_path, moved_to_path, redirect_chain 
FROM view_url_moves;

-- Missing URLs in staging
SELECT path FROM view_urls_missing;
```

## 🚀 Performance Tips

- Use `--concurrency` to match your server's capacity
- Enable `--js` only when necessary (slower but captures dynamic content)
- Use `--max-pages` for testing to avoid long crawls
- Set appropriate `--delay` to be respectful to target servers
- Use content hashing to identify duplicate content efficiently

## 📚 Documentation

- [Roadmap](ROADMAP.md) - Development roadmap and feature planning
- [Contributing Guidelines](CONTRIBUTING.md) - How to contribute to the project

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for:
- Feature request process
- Development setup
- Code standards
- Testing requirements

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

