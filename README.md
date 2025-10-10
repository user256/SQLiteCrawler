# SQLiteCrawler

A high-performance, persistent web crawler built with Python and SQLite. Features intelligent URL discovery, redirect tracking, content extraction, and comprehensive data storage with optimized performance settings.

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

## 📊 Performance

SQLiteCrawler uses optimized settings for maximum performance:
- **Parallel database operations** for faster I/O
- **Optimized batch sizes** (100/500/1000/100) for better throughput
- **Intelligent retry logic** with exponential backoff
- **7.1% faster** than baseline configuration

See [PERFORMANCE_ANALYSIS.md](PERFORMANCE_ANALYSIS.md) for detailed benchmark results.

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

### **Crawl Comparison** 🆕
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

### **Crawl Comparison** 🆕

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

**Output:** Creates a comparison database with analysis views for detailed reporting.

## 🗄️ Database Schema

### **Pages Database (`*_pages.db`)**
- `pages`: Raw HTML content and HTTP headers
- `content`: Extracted content (titles, meta descriptions, H1/H2 tags, word counts)
- `schema_data`: Structured data (JSON-LD, microdata, RDFa)

### **Crawl Database (`*_crawl.db`)**
- `urls`: Discovered URLs with classification (internal/external/network/social)
- `frontier`: Crawl queue with priority scoring
- `internal_links`: Link relationships with anchor text and XPath
- `redirects`: Redirect chains and final destinations
- `hreflang_sitemap`: Hreflang data from sitemaps
- `robots_txt`: Robots.txt analysis and directives

### **Comparison Database (`*_vs_*_comparison.db`)**
- `comparison_urls`: URL mapping with content analysis and move tracking
- `comparison_sessions`: Comparison run metadata
- `commercial_pages`: Commercial page analysis (optional)

## 📊 Analysis Views

### **Standard Views**
- `view_crawl_overview`: Comprehensive crawl summary
- `view_internal_links`: Internal link analysis
- `view_network_links`: Network link analysis  
- `view_external_links`: External link analysis
- `view_schema_data`: Structured data analysis

### **Comparison Views** 🆕
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
FROM view_internal_links 
WHERE is_image = 1;
```

### **Comparison Analysis** 🆕
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

## 📚 Documentation

- [Performance Analysis](PERFORMANCE_ANALYSIS.md) - Detailed benchmark results
- [Crawl Improvements](crawl_improvements.md) - Core crawler enhancement roadmap
- [Comparison Improvements](comparison_improvements.md) - Comparison feature roadmap

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for:
- Feature request process
- Development setup
- Code standards
- Testing requirements

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📞 Support

- **Issues:** [GitHub Issues](https://github.com/user256/SQLiteCrawler/issues)
- **Documentation:** [Wiki](https://github.com/user256/SQLiteCrawler/wiki)
- **Community:** [Discussions](https://github.com/user256/SQLiteCrawler/discussions)

---

*SQLiteCrawler - High-performance web crawling with comprehensive analysis capabilities*