# SQLiteCrawler Roadmap

## 🎯 Current Status
**Version:** 0.3  

## 🚀 Immediate Priorities

### Database & Views
- [x] **Subdomain Classification** - Classify links to subdomains as 'subdomain' instead of 'external' ✅
- [x] **Link View Reorganization** - Consolidate into `view_links_internal`, `view_links_external`, `view_links_network`, `view_links_subdomain` ✅
- [x] **Remove Deprecated Views** - Drop `view_links_enhanced` and `view_anchor_links` ✅
- [x] **Create Hub View** - Show pages with >1 child, ordered by child count (descending) ✅
- [ ] **Fix Schema Validation** - Investigate schema validation issues

### Core Improvements
- [x] **Incremental Crawl Diffs** - Detect changes per URL using content hashes ✅
- [x] **Sitemap TTL Caching** - Implement sitemap freshness policy ✅
- [ ] **Enhanced Content Detection** - Better charset detection and client-side redirect handling
- [ ] **Retry Optimization** - Optimize retry/backoff by failure type (basic exponential backoff exists, but not optimized by failure type)

## 🔧 Technical Debt

### Code Quality
- [ ] **Function Refactoring** - Split large functions (comparison, schema extraction, content processing)
- [ ] **Error Handling** - Improve retry logic with proper backoff strategies
- [ ] **Caching Layer** - Add cache layer for repeated database lookups
- [ ] **URL Normalization** - Handle more tracking parameters and edge cases (UTM params handled, but fbclid/gclid TODO)

### Performance
- [ ] **Compression** - Consider Brotli compression for better ratios
- [ ] **Hash Algorithms** - Evaluate faster hashing (xxHash) for large schemas
- [ ] **Database Optimization** - Add PRAGMA optimize() for better query planning
- [ ] **PostgreSQL** - add support for PostgreSQL

## 🚀 Future Features

### Advanced Crawling
- [ ] **Proxy Rotation** - Support rotating proxies
- [ ] **Header Rotation** - Enable rotating headers
- [ ] **Custom Extraction** - CSS Path, XPath, regex data extraction
- [ ] **Migration System** - Semver schema migrations

### Analysis & Reporting
- [ ] **DB export** - export to biqquery or similar db 
- [ ] **DB export** - export to csv (from db or as crawl instruction) 
- [ ] **Enhanced Content Analysis** - Image analysis, schema markup comparison
- [ ] **Advanced Link Analysis** - Link equity, broken link detection, anchor text analysis
- [ ] **Performance Metrics** - Page load times, Core Web Vitals
- [ ] **SEO Analysis** - Technical SEO, mobile-friendliness, accessibility
- [ ] **Export Functionality** - CSV, JSON, PDF reports
- [ ] **Web Dashboard** - Visual comparison interface

## ✅ Completed Features

### Performance Optimizations
- ✅ Parallel database write operations (3.3x faster)
- ✅ Increased default batch sizes (70% faster)
- ✅ Adaptive politeness per host
- ✅ Conditional requests (ETag/If-Modified-Since)
- ✅ Frontier scoring with priority
- ✅ URL normalization hardening
- ✅ Optimized ThreadPoolExecutor settings
- ✅ Prefetch optimization
- ✅ HTML compression optimization
- ✅ Comprehensive database indexing (62 indexes)
- ✅ URL normalization caching (LRU cache with 10,000 entries)
- ✅ Content hashing (SHA256 + SimHash)
- ✅ Fragment table for better URL normalization

### Core Features
- ✅ HTTP/2 & Brotli support
- ✅ Enhanced authentication methods
- ✅ CSV crawl support
- ✅ Crawl comparison functionality
- ✅ Content analysis and URL move detection
- ✅ Comprehensive comparison views

### Data Model
- ✅ SQLite optimization (WAL mode, batching)
- ✅ Database normalization
- ✅ Redirect tracking
- ✅ Schema.org extraction
- ✅ Hreflang support
- ✅ Fragment table implementation
- ✅ Content hashing (SHA256 + SimHash)

## 📊 Success Metrics

### Performance Targets
- Comparison completion time: <50% of current
- Memory usage: <2GB for 10K page comparisons
- Database query performance: <100ms for complex views

### Quality Targets
- Issue detection accuracy: >95%
- False positive rate: <5%
- Content analysis coverage: >90%

### User Experience
- User satisfaction score: >4.5/5
- Feature adoption rate: >80% for core features
- Time saved per comparison: 75% reduction

## 🗓️ Release Timeline

### v0.4 - Performance & Quality (Q1 2025)
- URL normalization improvements
- Sitemap TTL caching
- Enhanced content detection
- Retry optimization

### v0.5 - Advanced Features (Q2 2025)
- Proxy rotation
- Custom extraction
- Enhanced comparison analysis
- Export functionality

### v0.6 - User Experience (Q3 2025)
- Web dashboard
- Advanced reporting
- API endpoints
- Team collaboration

---

## 📝 Development Notes

### Performance Insights
Async HTML parsing, ThreadPoolExecutor optimizations, and database connection pooling were tested but found to be slower than the current approach. Direct `aiosqlite.connect()` calls are more reliable and performant than connection pooling for SQLite.

### Architecture Decisions
- **Database**: SQLite with WAL mode for optimal performance
- **Concurrency**: Async/await with controlled concurrency limits
- **Caching**: LRU cache for URL normalization, in-memory for frequent lookups
- **Compression**: zlib level 9 with base64 encoding for HTML storage
