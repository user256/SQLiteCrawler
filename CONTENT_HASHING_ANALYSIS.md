# Content Hashing Analysis for SQLiteCrawler

## 🎯 Overview

Content hashing using SHA256 + SimHash/MinHash would provide significant value for both **crawl comparison** and **duplicate detection**. This analysis outlines the implementation approach and benefits.

## 🔍 Use Cases

### **1. Crawl Comparison**
- **Content Change Detection** - Identify pages with identical content between origin/staging
- **Duplicate Content Analysis** - Find pages with similar content across domains
- **Content Migration Tracking** - Detect when content moves between URLs
- **Staging Validation** - Ensure staging content matches production

### **2. Duplicate Detection**
- **Internal Duplicates** - Find pages with identical/similar content within a site
- **Near-Duplicate Detection** - Identify pages with minor variations (templates, ads, etc.)
- **Content Quality Analysis** - Flag thin/duplicate content issues
- **SEO Optimization** - Identify canonicalization opportunities

## 🛠️ Implementation Approach

### **Phase 1: Basic Content Hashing**

#### **Database Schema Extensions**
```sql
-- Add content hash columns to existing tables
ALTER TABLE content ADD COLUMN content_hash_sha256 TEXT;
ALTER TABLE content ADD COLUMN content_hash_simhash TEXT;
ALTER TABLE content ADD COLUMN content_length INTEGER;

-- Create indexes for hash lookups
CREATE INDEX IF NOT EXISTS idx_content_hash_sha256 ON content(content_hash_sha256);
CREATE INDEX IF NOT EXISTS idx_content_hash_simhash ON content(content_hash_simhash);
```

#### **Content Processing**
```python
import hashlib
from simhash import Simhash

def generate_content_hashes(html_content: str) -> dict:
    """Generate SHA256 and SimHash for content analysis."""
    # Clean content for hashing (remove dynamic elements)
    cleaned_content = clean_content_for_hashing(html_content)
    
    # SHA256 for exact duplicates
    sha256_hash = hashlib.sha256(cleaned_content.encode('utf-8')).hexdigest()
    
    # SimHash for near-duplicates
    simhash_value = Simhash(cleaned_content).value
    
    return {
        'content_hash_sha256': sha256_hash,
        'content_hash_simhash': str(simhash_value),
        'content_length': len(cleaned_content)
    }

def clean_content_for_hashing(html: str) -> str:
    """Clean HTML content for consistent hashing."""
    from bs4 import BeautifulSoup
    
    soup = BeautifulSoup(html, 'html.parser')
    
    # Remove dynamic elements
    for tag in soup.find_all(['script', 'style', 'noscript']):
        tag.decompose()
    
    # Remove attributes that change frequently
    for tag in soup.find_all():
        # Keep only essential attributes
        essential_attrs = ['href', 'src', 'alt', 'title']
        attrs_to_remove = [attr for attr in tag.attrs if attr not in essential_attrs]
        for attr in attrs_to_remove:
            del tag.attrs[attr]
    
    # Get text content with structure
    return soup.get_text(separator=' ', strip=True)
```

### **Phase 2: Crawl Comparison Integration**

#### **Comparison Database Extensions**
```sql
-- Add hash comparison to comparison_urls table
ALTER TABLE comparison_urls ADD COLUMN origin_content_hash_sha256 TEXT;
ALTER TABLE comparison_urls ADD COLUMN staging_content_hash_sha256 TEXT;
ALTER TABLE comparison_urls ADD COLUMN origin_content_hash_simhash TEXT;
ALTER TABLE comparison_urls ADD COLUMN staging_content_hash_simhash TEXT;
ALTER TABLE comparison_urls ADD COLUMN content_similarity_score REAL;

-- Create indexes for hash comparisons
CREATE INDEX IF NOT EXISTS idx_comparison_origin_sha256 ON comparison_urls(origin_content_hash_sha256);
CREATE INDEX IF NOT EXISTS idx_comparison_staging_sha256 ON comparison_urls(staging_content_hash_sha256);
```

#### **Comparison Views**
```sql
-- View: Exact content matches
CREATE VIEW IF NOT EXISTS view_content_exact_matches AS
SELECT 
    cu.path,
    cu.origin_content_hash_sha256,
    cu.staging_content_hash_sha256,
    'Exact match' as match_type
FROM comparison_urls cu
WHERE cu.origin_content_hash_sha256 = cu.staging_content_hash_sha256
  AND cu.origin_content_hash_sha256 IS NOT NULL;

-- View: Near-duplicate content
CREATE VIEW IF NOT EXISTS view_content_near_duplicates AS
SELECT 
    cu.path,
    cu.origin_content_hash_simhash,
    cu.staging_content_hash_simhash,
    cu.content_similarity_score,
    'Near duplicate' as match_type
FROM comparison_urls cu
WHERE cu.content_similarity_score > 0.8
  AND cu.content_similarity_score < 1.0;

-- View: Content changes
CREATE VIEW IF NOT EXISTS view_content_changes AS
SELECT 
    cu.path,
    cu.origin_content_hash_sha256,
    cu.staging_content_hash_sha256,
    'Content changed' as change_type
FROM comparison_urls cu
WHERE cu.origin_content_hash_sha256 != cu.staging_content_hash_sha256
  AND cu.origin_content_hash_sha256 IS NOT NULL
  AND cu.staging_content_hash_sha256 IS NOT NULL;
```

### **Phase 3: Duplicate Detection**

#### **Duplicate Analysis Views**
```sql
-- View: Internal exact duplicates
CREATE VIEW IF NOT EXISTS view_internal_exact_duplicates AS
SELECT 
    c1.url_id as url1_id,
    c2.url_id as url2_id,
    u1.url as url1,
    u2.url as url2,
    c1.content_hash_sha256,
    'Exact duplicate' as duplicate_type
FROM content c1
JOIN content c2 ON c1.content_hash_sha256 = c2.content_hash_sha256
JOIN urls u1 ON c1.url_id = u1.id
JOIN urls u2 ON c2.url_id = u2.id
WHERE c1.url_id < c2.url_id;  -- Avoid self-comparison and duplicates

-- View: Internal near-duplicates
CREATE VIEW IF NOT EXISTS view_internal_near_duplicates AS
SELECT 
    c1.url_id as url1_id,
    c2.url_id as url2_id,
    u1.url as url1,
    u2.url as url2,
    c1.content_hash_simhash,
    c2.content_hash_simhash,
    'Near duplicate' as duplicate_type
FROM content c1
JOIN content c2 ON c1.content_hash_simhash = c2.content_hash_simhash
JOIN urls u1 ON c1.url_id = u1.id
JOIN urls u2 ON c2.url_id = u2.id
WHERE c1.url_id < c2.url_id;
```

## 📊 Benefits

### **Crawl Comparison Benefits**
- **Content Change Detection** - Instantly identify pages with identical content
- **Migration Validation** - Verify content moved correctly between URLs
- **Staging Quality Assurance** - Ensure staging matches production content
- **Content Audit** - Identify unexpected content changes

### **Duplicate Detection Benefits**
- **SEO Optimization** - Identify canonicalization opportunities
- **Content Quality** - Flag thin/duplicate content issues
- **Site Architecture** - Understand content distribution patterns
- **Performance** - Avoid crawling duplicate content

### **Performance Benefits**
- **Fast Comparisons** - Hash-based lookups are O(log n)
- **Efficient Storage** - Hashes are much smaller than full content
- **Incremental Analysis** - Only hash new/changed content
- **Scalable** - Works with large crawls (10K+ pages)

## 🚀 Implementation Priority

### **High Priority (Next Release)**
1. **Basic content hashing** - SHA256 + SimHash generation
2. **Database schema updates** - Add hash columns and indexes
3. **Crawl comparison integration** - Hash comparison in comparison views
4. **Basic duplicate detection** - Internal duplicate analysis

### **Medium Priority (Future Release)**
1. **Advanced similarity scoring** - Configurable similarity thresholds
2. **Content change tracking** - Historical hash comparison
3. **Export functionality** - Duplicate reports and recommendations
4. **Performance optimization** - Batch hash processing

## 🔧 Technical Considerations

### **Dependencies**
```python
# Add to requirements
simhash==2.1.2  # For near-duplicate detection
```

### **Performance Impact**
- **Minimal** - Hashing is fast and happens during content extraction
- **Storage** - ~64 bytes per page (SHA256 + SimHash)
- **Memory** - SimHash library is lightweight
- **CPU** - Negligible impact on crawl performance

### **Accuracy Considerations**
- **Content Cleaning** - Remove dynamic elements for consistent hashing
- **SimHash Parameters** - Tune for optimal near-duplicate detection
- **Threshold Tuning** - Adjust similarity scores for different use cases

## 📈 Success Metrics

### **Crawl Comparison**
- **Content Change Detection** - 95% accuracy in identifying changed content
- **Migration Validation** - 100% accuracy in content move detection
- **Performance** - <1ms per hash comparison

### **Duplicate Detection**
- **Duplicate Identification** - 90% accuracy in finding internal duplicates
- **SEO Impact** - Identify 80% of canonicalization opportunities
- **Content Quality** - Flag 95% of thin content issues

---

*This analysis provides a comprehensive roadmap for implementing content hashing in SQLiteCrawler, with clear benefits for both crawl comparison and duplicate detection use cases.*
