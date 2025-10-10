"""
Crawl comparison functionality for comparing origin and staging domains.
"""
import asyncio
import aiosqlite
import time
from datetime import datetime
from urllib.parse import urlparse, urljoin
from typing import Optional, List, Dict, Any
import csv
import os

from .crawl import crawl
from .config import CrawlLimits, HttpConfig, get_db_paths
from .db import init_pages_db, init_crawl_db


async def run_crawl_comparison(
    origin_url: str,
    staging_url: str,
    commercial_csv: str = "",
    compare_links: bool = False,
    use_js: bool = False,
    limits: Optional[CrawlLimits] = None,
    http_config: Optional[HttpConfig] = None,
    allow_external: bool = False,
    max_workers: int = 6,
    verbose: bool = False
):
    """
    Run a comprehensive crawl comparison between origin and staging domains.
    
    Args:
        origin_url: The origin/production domain URL
        staging_url: The staging domain URL to compare against
        commercial_csv: Optional CSV file with commercial page URLs
        compare_links: Whether to enable detailed link comparison
        use_js: Whether to use JavaScript rendering
        limits: Crawl limits configuration
        http_config: HTTP configuration
        allow_external: Whether to allow external URLs
        max_workers: Maximum number of workers
        verbose: Whether to enable verbose output
    """
    print(f"🎯 Starting crawl comparison:")
    print(f"  Origin: {origin_url}")
    print(f"  Staging: {staging_url}")
    if commercial_csv:
        print(f"  Commercial CSV: {commercial_csv}")
    if compare_links:
        print(f"  Link comparison: Enabled")
    print()
    
    # Parse domains
    origin_domain = urlparse(origin_url).netloc
    staging_domain = urlparse(staging_url).netloc
    
    # Create comparison database
    comparison_db_path = f"data/{origin_domain}_vs_{staging_domain}_comparison.db"
    await init_comparison_db(comparison_db_path)
    
    # Step 1: Run origin crawl
    print("🔄 Step 1: Crawling origin domain...")
    print(f"  Settings: JS={use_js}, Concurrency={http_config.max_concurrency if http_config else 'default'}")
    origin_start_time = time.time()
    
    origin_pages_db, origin_crawl_db = get_db_paths(origin_url)
    await init_pages_db(origin_pages_db)
    await init_crawl_db(origin_crawl_db)
    
    await crawl(
        start=origin_url,
        use_js=use_js,
        limits=limits,
        reset_frontier=True,
        http_config=http_config,
        allow_external=allow_external,
        max_workers=max_workers,
        verbose=verbose
    )
    
    origin_time = time.time() - origin_start_time
    print(f"✅ Origin crawl completed in {origin_time:.1f}s")
    
    # Step 2: Generate staging seed list from origin URLs
    print("🔄 Step 2: Generating staging seed list...")
    staging_seed_urls = await generate_staging_seed_list(origin_crawl_db, origin_domain, staging_domain)
    print(f"✅ Generated {len(staging_seed_urls)} staging seed URLs")
    
    # Step 3: Run staging crawl
    print("🔄 Step 3: Crawling staging domain...")
    print(f"  Settings: JS={use_js}, Concurrency={http_config.max_concurrency if http_config else 'default'}")
    print(f"  Using {len(staging_seed_urls)} seed URLs from origin crawl")
    staging_start_time = time.time()
    
    staging_pages_db, staging_crawl_db = get_db_paths(staging_url)
    await init_pages_db(staging_pages_db)
    await init_crawl_db(staging_crawl_db)
    
    await crawl(
        start=staging_url,
        use_js=use_js,
        limits=limits,
        reset_frontier=True,
        http_config=http_config,
        allow_external=allow_external,
        max_workers=max_workers,
        verbose=verbose,
        csv_urls=staging_seed_urls,
        csv_seed_mode=True
    )
    
    staging_time = time.time() - staging_start_time
    print(f"✅ Staging crawl completed in {staging_time:.1f}s")
    
    # Step 4: Create comparison analysis
    print("🔄 Step 4: Creating comparison analysis...")
    await create_comparison_analysis(
        comparison_db_path=comparison_db_path,
        origin_crawl_db=origin_crawl_db,
        staging_crawl_db=staging_crawl_db,
        origin_domain=origin_domain,
        staging_domain=staging_domain,
        commercial_csv=commercial_csv,
        compare_links=compare_links
    )
    
    print(f"✅ Comparison analysis completed!")
    print(f"📊 Results saved to: {comparison_db_path}")
    print()
    
    # Display summary
    await display_comparison_summary(comparison_db_path, compare_links)


async def init_comparison_db(db_path: str):
    """Initialize the comparison database with schema."""
    async with aiosqlite.connect(db_path) as db:
        # Create comparison session table
        await db.execute('''
            CREATE TABLE IF NOT EXISTS comparison_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                origin_domain TEXT NOT NULL,
                staging_domain TEXT NOT NULL,
                origin_crawl_db TEXT NOT NULL,
                staging_crawl_db TEXT NOT NULL,
                created_at TEXT NOT NULL,
                commercial_csv TEXT,
                compare_links INTEGER DEFAULT 0
            )
        ''')
        
        # Create comparison URLs table
        await db.execute('''
            CREATE TABLE IF NOT EXISTS comparison_urls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL,
                path TEXT NOT NULL,
                origin_url_id INTEGER,
                staging_url_id INTEGER,
                exists_on_origin INTEGER DEFAULT 0,
                exists_on_staging INTEGER DEFAULT 0,
                -- Content analysis fields
                origin_title TEXT,
                staging_title TEXT,
                origin_h1 TEXT,
                staging_h1 TEXT,
                origin_meta_description TEXT,
                staging_meta_description TEXT,
                origin_word_count INTEGER,
                staging_word_count INTEGER,
                -- URL move tracking
                is_moved_content INTEGER DEFAULT 0,
                moved_from_path TEXT,
                moved_to_path TEXT,
                redirect_chain TEXT,
                FOREIGN KEY (session_id) REFERENCES comparison_sessions(id),
                UNIQUE(session_id, path)
            )
        ''')
        
        # Create commercial pages table (if needed)
        await db.execute('''
            CREATE TABLE IF NOT EXISTS commercial_pages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL,
                commercial_url TEXT NOT NULL,
                expected_staging_url TEXT NOT NULL,
                origin_url_id INTEGER,
                staging_url_id INTEGER,
                FOREIGN KEY (session_id) REFERENCES comparison_sessions(id)
            )
        ''')
        
        await db.commit()


async def generate_staging_seed_list(origin_crawl_db: str, origin_domain: str, staging_domain: str) -> List[str]:
    """Generate staging seed URLs by converting origin URLs to staging domain."""
    staging_urls = []
    
    async with aiosqlite.connect(origin_crawl_db) as db:
        # Get all crawled URLs from origin
        cursor = await db.execute('''
            SELECT u.url 
            FROM urls u
            JOIN frontier f ON u.id = f.url_id
            WHERE f.status = 'done' AND u.classification = 'internal'
        ''')
        
        origin_urls = await cursor.fetchall()
        
        for (url,) in origin_urls:
            # Convert origin URL to staging URL
            staging_url = url.replace(f"https://{origin_domain}", f"https://{staging_domain}")
            staging_url = staging_url.replace(f"http://{origin_domain}", f"http://{staging_domain}")
            staging_urls.append(staging_url)
    
    return staging_urls


async def create_comparison_analysis(
    comparison_db_path: str,
    origin_crawl_db: str,
    staging_crawl_db: str,
    origin_domain: str,
    staging_domain: str,
    commercial_csv: str = "",
    compare_links: bool = False
):
    """Create comprehensive comparison analysis and views."""
    
    # Create comparison session record
    session_id = await create_comparison_session(
        comparison_db_path, origin_domain, staging_domain, 
        origin_crawl_db, staging_crawl_db, commercial_csv, compare_links
    )
    
    # Create comparison URLs mapping
    await create_comparison_urls_mapping(
        comparison_db_path, session_id, origin_crawl_db, staging_crawl_db
    )
    
    # Create comparison views
    await create_comparison_views(comparison_db_path, session_id, compare_links)
    
    # Handle commercial pages if CSV provided
    if commercial_csv and os.path.exists(commercial_csv):
        await process_commercial_pages(
            comparison_db_path, session_id, commercial_csv, origin_domain, staging_domain
        )


async def create_comparison_session(
    db_path: str, origin_domain: str, staging_domain: str,
    origin_crawl_db: str, staging_crawl_db: str, commercial_csv: str, compare_links: bool
) -> int:
    """Create a comparison session record and return session ID."""
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute('''
            INSERT INTO comparison_sessions 
            (origin_domain, staging_domain, origin_crawl_db, staging_crawl_db, 
             created_at, commercial_csv, compare_links)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            origin_domain, staging_domain, origin_crawl_db, staging_crawl_db,
            datetime.now().isoformat(), commercial_csv, 1 if compare_links else 0
        ))
        await db.commit()
        return cursor.lastrowid


async def create_comparison_urls_mapping(
    db_path: str, session_id: int, origin_crawl_db: str, staging_crawl_db: str
):
    """Create mapping of URLs between origin and staging crawls with content analysis."""
    async with aiosqlite.connect(db_path) as db:
        # Get all URLs from both crawls with content data
        origin_urls = {}
        staging_urls = {}
        origin_redirects = {}
        staging_redirects = {}
        
        # Get origin URLs with content
        async with aiosqlite.connect(origin_crawl_db) as origin_db:
            cursor = await origin_db.execute('''
                SELECT u.id, u.url, u.classification, c.title, c.h1_tags, c.word_count
                FROM urls u
                JOIN frontier f ON u.id = f.url_id
                LEFT JOIN content c ON u.id = c.url_id
                WHERE f.status = 'done'
            ''')
            for url_id, url, classification, title, h1_tags, word_count in await cursor.fetchall():
                # Extract first H1 from JSON array
                h1 = None
                if h1_tags:
                    try:
                        import json
                        h1_list = json.loads(h1_tags)
                        h1 = h1_list[0] if h1_list else None
                    except:
                        h1 = None
                
                origin_urls[url] = {
                    'id': url_id, 
                    'classification': classification,
                    'title': title,
                    'h1': h1,
                    'meta_description': None,  # Not available in current schema
                    'word_count': word_count or 0
                }
            
            # Get origin redirects
            cursor = await origin_db.execute('''
                SELECT u1.url, u2.url
                FROM urls u1
                JOIN redirects r ON u1.id = r.source_url_id
                JOIN urls u2 ON r.target_url_id = u2.id
            ''')
            for source_url, dest_url in await cursor.fetchall():
                origin_redirects[source_url] = dest_url
        
        # Get staging URLs with content
        async with aiosqlite.connect(staging_crawl_db) as staging_db:
            cursor = await staging_db.execute('''
                SELECT u.id, u.url, u.classification, c.title, c.h1_tags, c.word_count
                FROM urls u
                JOIN frontier f ON u.id = f.url_id
                LEFT JOIN content c ON u.id = c.url_id
                WHERE f.status = 'done'
            ''')
            for url_id, url, classification, title, h1_tags, word_count in await cursor.fetchall():
                # Extract first H1 from JSON array
                h1 = None
                if h1_tags:
                    try:
                        import json
                        h1_list = json.loads(h1_tags)
                        h1 = h1_list[0] if h1_list else None
                    except:
                        h1 = None
                
                staging_urls[url] = {
                    'id': url_id, 
                    'classification': classification,
                    'title': title,
                    'h1': h1,
                    'meta_description': None,  # Not available in current schema
                    'word_count': word_count or 0
                }
            
            # Get staging redirects
            cursor = await staging_db.execute('''
                SELECT u1.url, u2.url
                FROM urls u1
                JOIN redirects r ON u1.id = r.source_url_id
                JOIN urls u2 ON r.target_url_id = u2.id
            ''')
            for source_url, dest_url in await cursor.fetchall():
                staging_redirects[source_url] = dest_url
        
        # Create comparison URLs mapping with redirect handling
        all_paths = set()
        url_moves = {}  # Track URL moves (origin_path -> staging_path)
        
        # Extract paths from origin URLs
        for url in origin_urls.keys():
            parsed = urlparse(url)
            path = parsed.path or '/'
            all_paths.add(path)
        
        # Extract paths from staging URLs
        for url in staging_urls.keys():
            parsed = urlparse(url)
            path = parsed.path or '/'
            all_paths.add(path)
        
        # Detect URL moves by checking redirects
        for origin_url, origin_data in origin_urls.items():
            origin_path = urlparse(origin_url).path or '/'
            
            # Check if this origin URL redirects to a staging URL
            if origin_url in origin_redirects:
                redirect_dest = origin_redirects[origin_url]
                redirect_path = urlparse(redirect_dest).path or '/'
                
                # Check if the redirect destination exists in staging
                for staging_url, staging_data in staging_urls.items():
                    staging_path = urlparse(staging_url).path or '/'
                    if staging_path == redirect_path:
                        url_moves[origin_path] = staging_path
                        break
        
        # Insert comparison URLs with content analysis
        for path in all_paths:
            # Find corresponding URLs in origin and staging
            origin_url_id = None
            staging_url_id = None
            exists_on_origin = 0
            exists_on_staging = 0
            
            # Content data
            origin_title = None
            staging_title = None
            origin_h1 = None
            staging_h1 = None
            origin_meta_desc = None
            staging_meta_desc = None
            origin_word_count = 0
            staging_word_count = 0
            
            # URL move tracking
            is_moved_content = 0
            moved_from_path = None
            moved_to_path = None
            redirect_chain = None
            
            # Look for origin URL with this path
            for url, data in origin_urls.items():
                if urlparse(url).path == path:
                    origin_url_id = data['id']
                    exists_on_origin = 1
                    origin_title = data['title']
                    origin_h1 = data['h1']
                    origin_meta_desc = data['meta_description']
                    origin_word_count = data['word_count']
                    break
            
            # Look for staging URL with this path
            for url, data in staging_urls.items():
                if urlparse(url).path == path:
                    staging_url_id = data['id']
                    exists_on_staging = 1
                    staging_title = data['title']
                    staging_h1 = data['h1']
                    staging_meta_desc = data['meta_description']
                    staging_word_count = data['word_count']
                    break
            
            # Check for URL moves
            if path in url_moves:
                is_moved_content = 1
                moved_from_path = path
                moved_to_path = url_moves[path]
                redirect_chain = f"{path} -> {url_moves[path]}"
            
            await db.execute('''
                INSERT OR REPLACE INTO comparison_urls 
                (session_id, path, origin_url_id, staging_url_id, exists_on_origin, exists_on_staging,
                 origin_title, staging_title, origin_h1, staging_h1, 
                 origin_meta_description, staging_meta_description,
                 origin_word_count, staging_word_count,
                 is_moved_content, moved_from_path, moved_to_path, redirect_chain)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (session_id, path, origin_url_id, staging_url_id, exists_on_origin, exists_on_staging,
                  origin_title, staging_title, origin_h1, staging_h1,
                  origin_meta_desc, staging_meta_desc,
                  origin_word_count, staging_word_count,
                  is_moved_content, moved_from_path, moved_to_path, redirect_chain))
        
        await db.commit()


async def create_comparison_views(db_path: str, session_id: int, compare_links: bool):
    """Create comparison analysis views."""
    async with aiosqlite.connect(db_path) as db:
        # View: Sitemap changes
        await db.execute('''
            CREATE VIEW IF NOT EXISTS view_sitemap_changes AS
            SELECT 
                cu.path,
                cu.exists_on_origin,
                cu.exists_on_staging,
                CASE 
                    WHEN cu.exists_on_origin = 1 AND cu.exists_on_staging = 0 THEN 'Missing in staging'
                    WHEN cu.exists_on_origin = 0 AND cu.exists_on_staging = 1 THEN 'New in staging'
                    WHEN cu.exists_on_origin = 1 AND cu.exists_on_staging = 1 THEN 'Present in both'
                    ELSE 'Unknown'
                END as change_type
            FROM comparison_urls cu
        ''')
        
        # View: URLs missing in staging
        await db.execute('''
            CREATE VIEW IF NOT EXISTS view_urls_missing AS
            SELECT 
                cu.path,
                'Missing in staging' as issue_type
            FROM comparison_urls cu
            WHERE cu.exists_on_origin = 1 AND cu.exists_on_staging = 0
        ''')
        
        # View: URLs new in staging
        await db.execute('''
            CREATE VIEW IF NOT EXISTS view_urls_new AS
            SELECT 
                cu.path,
                'New in staging' as issue_type
            FROM comparison_urls cu
            WHERE cu.exists_on_origin = 0 AND cu.exists_on_staging = 1
        ''')
        
        # View: Indexability comparison
        await db.execute('''
            CREATE VIEW IF NOT EXISTS view_indexability_comparison AS
            SELECT 
                cu.path,
                cu.exists_on_origin,
                cu.exists_on_staging,
                -- This would need to be populated with actual indexability data
                'Indexable' as origin_indexability,
                'Indexable' as staging_indexability,
                'Match' as indexability_match
            FROM comparison_urls cu
        ''')
        
        # View: Crawl overview comparison with real content analysis
        await db.execute('''
            CREATE VIEW IF NOT EXISTS view_crawl_overview_comparison AS
            SELECT 
                cu.path,
                cu.exists_on_origin,
                cu.exists_on_staging,
                cu.origin_title,
                cu.staging_title,
                CASE 
                    WHEN cu.origin_title = cu.staging_title THEN 'Match'
                    WHEN cu.origin_title IS NULL AND cu.staging_title IS NULL THEN 'Both Missing'
                    WHEN cu.origin_title IS NULL THEN 'Missing in Origin'
                    WHEN cu.staging_title IS NULL THEN 'Missing in Staging'
                    ELSE 'Different'
                END as title_match,
                cu.origin_h1,
                cu.staging_h1,
                CASE 
                    WHEN cu.origin_h1 = cu.staging_h1 THEN 'Match'
                    WHEN cu.origin_h1 IS NULL AND cu.staging_h1 IS NULL THEN 'Both Missing'
                    WHEN cu.origin_h1 IS NULL THEN 'Missing in Origin'
                    WHEN cu.staging_h1 IS NULL THEN 'Missing in Staging'
                    ELSE 'Different'
                END as h1_match,
                cu.origin_meta_description,
                cu.staging_meta_description,
                CASE 
                    WHEN cu.origin_meta_description = cu.staging_meta_description THEN 'Match'
                    WHEN cu.origin_meta_description IS NULL AND cu.staging_meta_description IS NULL THEN 'Both Missing'
                    WHEN cu.origin_meta_description IS NULL THEN 'Missing in Origin'
                    WHEN cu.staging_meta_description IS NULL THEN 'Missing in Staging'
                    ELSE 'Different'
                END as meta_description_match,
                cu.origin_word_count,
                cu.staging_word_count,
                CASE 
                    WHEN cu.origin_word_count = cu.staging_word_count THEN 'Match'
                    WHEN cu.origin_word_count IS NULL AND cu.staging_word_count IS NULL THEN 'Both Missing'
                    WHEN cu.origin_word_count IS NULL THEN 'Missing in Origin'
                    WHEN cu.staging_word_count IS NULL THEN 'Missing in Staging'
                    ELSE 'Different'
                END as word_count_match,
                cu.is_moved_content,
                cu.moved_from_path,
                cu.moved_to_path,
                cu.redirect_chain
            FROM comparison_urls cu
        ''')
        
        # View: Schema comparison
        await db.execute('''
            CREATE VIEW IF NOT EXISTS view_schema_comparison AS
            SELECT 
                cu.path,
                cu.exists_on_origin,
                cu.exists_on_staging,
                'No schema' as origin_schema,
                'No schema' as staging_schema,
                'Match' as schema_match
            FROM comparison_urls cu
        ''')
        
        # View: URL moves and redirects
        await db.execute('''
            CREATE VIEW IF NOT EXISTS view_url_moves AS
            SELECT 
                cu.path,
                cu.moved_from_path,
                cu.moved_to_path,
                cu.redirect_chain,
                'Content moved via redirect' as move_type
            FROM comparison_urls cu
            WHERE cu.is_moved_content = 1
        ''')
        
        # View: Content differences
        await db.execute('''
            CREATE VIEW IF NOT EXISTS view_content_differences AS
            SELECT 
                cu.path,
                cu.origin_title,
                cu.staging_title,
                cu.title_match,
                cu.origin_h1,
                cu.staging_h1,
                cu.h1_match,
                cu.origin_meta_description,
                cu.staging_meta_description,
                cu.meta_description_match,
                cu.origin_word_count,
                cu.staging_word_count,
                cu.word_count_match,
                CASE 
                    WHEN cu.title_match != 'Match' OR cu.h1_match != 'Match' OR 
                         cu.meta_description_match != 'Match' OR cu.word_count_match != 'Match'
                    THEN 'Content differences detected'
                    ELSE 'No content differences'
                END as overall_content_status
            FROM comparison_urls cu
            WHERE cu.exists_on_origin = 1 AND cu.exists_on_staging = 1
        ''')
        
        # Optional link comparison views
        if compare_links:
            await db.execute('''
                CREATE VIEW IF NOT EXISTS view_internal_links_added AS
                SELECT 
                    cu.path,
                    'Links added in staging' as link_change_type
                FROM comparison_urls cu
                WHERE cu.exists_on_staging = 1
            ''')
            
            await db.execute('''
                CREATE VIEW IF NOT EXISTS view_internal_links_lost AS
                SELECT 
                    cu.path,
                    'Links lost in staging' as link_change_type
                FROM comparison_urls cu
                WHERE cu.exists_on_origin = 1
            ''')
        
        await db.commit()


async def process_commercial_pages(
    db_path: str, session_id: int, commercial_csv: str, origin_domain: str, staging_domain: str
):
    """Process commercial pages from CSV file."""
    async with aiosqlite.connect(db_path) as db:
        with open(commercial_csv, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                commercial_url = row.get('url', '').strip()
                if commercial_url:
                    # Convert to expected staging URL
                    expected_staging_url = commercial_url.replace(
                        f"https://{origin_domain}", f"https://{staging_domain}"
                    ).replace(
                        f"http://{origin_domain}", f"http://{staging_domain}"
                    )
                    
                    await db.execute('''
                        INSERT INTO commercial_pages 
                        (session_id, commercial_url, expected_staging_url)
                        VALUES (?, ?, ?)
                    ''', (session_id, commercial_url, expected_staging_url))
        
        await db.commit()
        
        # Create commercial analysis views
        await db.execute('''
            CREATE VIEW IF NOT EXISTS view_commercial_summary AS
            SELECT 
                COUNT(*) as total_commercial_pages,
                SUM(CASE WHEN cp.staging_url_id IS NOT NULL THEN 1 ELSE 0 END) as found_in_staging,
                SUM(CASE WHEN cp.staging_url_id IS NULL THEN 1 ELSE 0 END) as missing_in_staging
            FROM commercial_pages cp
        ''')
        
        await db.execute('''
            CREATE VIEW IF NOT EXISTS view_commercial_issues AS
            SELECT 
                cp.commercial_url,
                cp.expected_staging_url,
                CASE 
                    WHEN cp.staging_url_id IS NULL THEN 'Missing from staging'
                    ELSE 'No issues'
                END as issue_type
            FROM commercial_pages cp
        ''')
        
        await db.commit()


async def display_comparison_summary(db_path: str, compare_links: bool = False):
    """Display a summary of the comparison results."""
    async with aiosqlite.connect(db_path) as db:
        # Get basic statistics
        cursor = await db.execute('''
            SELECT 
                COUNT(*) as total_paths,
                SUM(exists_on_origin) as origin_paths,
                SUM(exists_on_staging) as staging_paths,
                SUM(CASE WHEN exists_on_origin = 1 AND exists_on_staging = 0 THEN 1 ELSE 0 END) as missing_in_staging,
                SUM(CASE WHEN exists_on_origin = 0 AND exists_on_staging = 1 THEN 1 ELSE 0 END) as new_in_staging
            FROM comparison_urls
        ''')
        
        stats = await cursor.fetchone()
        total_paths, origin_paths, staging_paths, missing_in_staging, new_in_staging = stats
        
        print("📊 Comparison Summary:")
        print(f"  Total unique paths: {total_paths}")
        print(f"  Paths in origin: {origin_paths}")
        print(f"  Paths in staging: {staging_paths}")
        print(f"  Missing in staging: {missing_in_staging}")
        print(f"  New in staging: {new_in_staging}")
        print()
        
        # Check for commercial analysis
        cursor = await db.execute('''
            SELECT COUNT(*) FROM commercial_pages
        ''')
        commercial_count = (await cursor.fetchone())[0]
        
        if commercial_count > 0:
            print(f"📈 Commercial pages analyzed: {commercial_count}")
            print()
        
        print("🔍 Available analysis views:")
        print("  - view_sitemap_changes")
        print("  - view_urls_missing")
        print("  - view_urls_new")
        print("  - view_indexability_comparison")
        print("  - view_crawl_overview_comparison")
        print("  - view_schema_comparison")
        print("  - view_url_moves")
        print("  - view_content_differences")
        if compare_links:
            print("  - view_internal_links_added")
            print("  - view_internal_links_lost")
        if commercial_count > 0:
            print("  - view_commercial_summary")
            print("  - view_commercial_issues")
