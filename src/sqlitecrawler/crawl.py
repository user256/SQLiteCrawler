from __future__ import annotations
import asyncio
import signal
import sys
from urllib.parse import urlsplit, urlparse, urlunparse
from typing import Iterable, Tuple, List, Optional
from concurrent.futures import ThreadPoolExecutor
from .config import HttpConfig, CrawlLimits, get_db_paths
from .db import (
    init_pages_db,
    init_crawl_db,
    write_page,
    upsert_url,
    frontier_seed,
    frontier_next_batch,
    frontier_mark_done,
    frontier_enqueue_many,
    frontier_stats,
    batch_write_pages,
    batch_upsert_urls,
    batch_enqueue_frontier,
    batch_write_content,
    batch_write_content_with_url_resolution,
    batch_write_hreflang_sitemap_data,
    batch_write_sitemaps_listed,
    batch_write_redirects,
    batch_write_internal_links,
    extract_content_from_html,
)
from .fetch import fetch_many, fetch_many_with_redirect_tracking
from .parse import classify, extract_links_from_html, extract_links_with_metadata, extract_from_sitemap
from .robots import discover_sitemaps_from_domain, crawl_sitemaps_recursive, parse_robots_txt

def _same_host(a: str, b: str) -> bool:
    return urlsplit(a).netloc.lower() == urlsplit(b).netloc.lower()

def normalize_url_for_storage(url: str) -> str:
    """Normalize URL for storage to minimize duplicates and file size."""
    parsed = urlparse(url)
    # Convert to lowercase
    normalized = urlunparse((
        parsed.scheme.lower(),
        parsed.netloc.lower(),
        parsed.path,
        parsed.params,
        parsed.query,
        ''  # Remove fragment
    ))
    # Preserve trailing slashes - don't remove them
    return normalized

def normalize_headers(headers: dict) -> dict:
    """Normalize headers to minimize storage size."""
    normalized = {}
    for key, value in headers.items():
        # Convert header names to lowercase
        key_lower = key.lower()
        # Only store essential headers to save space
        if key_lower in {'content-type', 'content-length', 'last-modified', 'etag', 'server'}:
            normalized[key_lower] = str(value).strip()
    return normalized

def should_crawl_url(url: str, base_domain: str, allow_external: bool, is_from_sitemap: bool = False, user_agent: str = "SQLiteCrawler/0.2") -> bool:
    """Determine if a URL should be crawled based on classification and settings."""
    from .db import classify_url
    from .robots import is_url_crawlable
    
    classification = classify_url(url, base_domain, is_from_sitemap)
    
    # Always crawl internal URLs (but check robots.txt)
    if classification == 'internal':
        return is_url_crawlable(url, user_agent)
    
    # Always crawl network URLs (from sitemaps, but check robots.txt)
    if classification == 'network':
        return is_url_crawlable(url, user_agent)
    
    # Never crawl social media URLs
    if classification == 'social':
        return False
    
    # External URLs only if explicitly allowed
    if classification == 'external':
        return allow_external
    
    return False

# Global flag for graceful shutdown
shutdown_requested = False

def signal_handler(signum, frame):
    """Handle interrupt signals for graceful shutdown."""
    global shutdown_requested
    print(f"\nReceived signal {signum}. Gracefully shutting down...")
    print("Press Ctrl+C again to force quit.")
    shutdown_requested = True

async def crawl(start: str, use_js: bool = False, limits: CrawlLimits | None = None, reset_frontier: bool = False, http_config: HttpConfig | None = None, allow_external: bool = False, max_workers: int = 4, verbose: bool = False):
    """Persistent breadth-first crawl with pause/resume.
    - Seeds the frontier if empty (or `reset_frontier=True`).
    - Respects `limits.max_pages`, `limits.max_depth`, and `limits.same_host_only`.
    - Stores pages in website-specific pages.db and discovered URLs/types in website-specific crawl.db.
    - Supports graceful shutdown with Ctrl+C (SIGINT) or SIGTERM.
    """
    global shutdown_requested
    
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    cfg = http_config or HttpConfig()
    limits = limits or CrawlLimits()
    
    # Extract base domain for URL classification
    from urllib.parse import urlparse
    base_domain = urlparse(start).netloc.lower()
    
    # Get website-specific database paths
    pages_db_path, crawl_db_path = get_db_paths(start)
    print(f"Using databases: {pages_db_path}, {crawl_db_path}")
    print(f"Base domain: {base_domain}, Allow external: {allow_external}")

    await init_pages_db(pages_db_path)
    await init_crawl_db(crawl_db_path)

    # Skip sitemap discovery if requested
    if http_config.skip_sitemaps:
        print("Skipping sitemap discovery (--skip-sitemaps enabled)")
        sitemap_urls = []
        sitemap_urls_dict = {}
    else:
        # Parse robots.txt for sitemap discovery (unless skipped)
        if not http_config.skip_robots_sitemaps:
            print(f"Parsing robots.txt for {base_domain}...")
            await parse_robots_txt(base_domain, http_config.user_agent, http_config)
        
        print(f"Discovering sitemaps for {base_domain}...")
        sitemap_urls = await discover_sitemaps_from_domain(base_domain, http_config.user_agent, http_config.skip_robots_sitemaps, http_config)
        
        # Crawl sitemaps to discover URLs
        if sitemap_urls:
            print("Crawling sitemaps to discover URLs...")
            sitemap_urls_dict, url_to_sitemap_mapping = await crawl_sitemaps_recursive(sitemap_urls, http_config.user_agent, verbose=verbose, http_config=http_config)
        else:
            sitemap_urls_dict = {}
            url_to_sitemap_mapping = {}
    
    if sitemap_urls:
        print(f"Found {len(sitemap_urls)} sitemap(s): {sitemap_urls}")
        
    # Always seed frontier with start URL FIRST (regardless of sitemap discovery)
    print(f"Adding start URL to frontier: {start}")
    start_norm = normalize_url_for_storage(start)
    await frontier_seed(start_norm, base_domain, reset=reset_frontier, db_path=crawl_db_path)

    if sitemap_urls_dict:
        print(f"Discovered {len(sitemap_urls_dict)} URLs from sitemaps")
        
        # First, add all sitemap URLs to the urls table so we can reference them for hreflang data
        sitemap_urls_to_upsert = []
        sitemap_tracking_data = []
        position = 0
        
        for url in sitemap_urls_dict.keys():
            url_norm = normalize_url_for_storage(url)
            # These are HTML pages discovered from sitemaps, not sitemap files themselves
            sitemap_urls_to_upsert.append((url_norm, "html", base_domain, None, True))  # is_from_sitemap=True
            # Track which sitemap this URL came from with position
            source_sitemap_url = url_to_sitemap_mapping.get(url, "unknown")
            sitemap_tracking_data.append((url_norm, source_sitemap_url, position))
            position += 1
        
        if sitemap_urls_to_upsert:
            print(f"Adding {len(sitemap_urls_to_upsert)} sitemap URLs to database...")
            await batch_upsert_urls(sitemap_urls_to_upsert, crawl_db_path)
            
            # Add sitemap tracking records
            if sitemap_tracking_data:
                print(f"Adding {len(sitemap_tracking_data)} sitemap tracking records...")
                await batch_write_sitemaps_listed(sitemap_tracking_data, crawl_db_path)
        
        # Process hreflang data from sitemaps
        hreflang_data_to_write = []
        for url, url_data in sitemap_urls_dict.items():
            if 'hreflangs' in url_data and 'hrefs' in url_data:
                # Normalize the URL for database lookup
                url_norm = normalize_url_for_storage(url)
                for hreflang, href in zip(url_data['hreflangs'], url_data['hrefs']):
                    if hreflang and href:
                        hreflang_data_to_write.append((url_norm, hreflang, href))
        
        if hreflang_data_to_write:
            print(f"Writing {len(hreflang_data_to_write)} hreflang entries to database...")
            await batch_write_hreflang_sitemap_data(hreflang_data_to_write, crawl_db_path)
        
        # Add sitemap URLs to frontier - AFTER start URL
        sitemap_urls_list = list(sitemap_urls_dict.keys())
        if limits.max_pages > 0:
            # If there's a limit, only add up to that many URLs
            for url in sitemap_urls_list[:limits.max_pages]:
                url_norm = normalize_url_for_storage(url)
                await frontier_seed(url_norm, base_domain, reset=False, db_path=crawl_db_path)
            print(f"Added {min(len(sitemap_urls_list), limits.max_pages)} URLs from sitemaps to frontier")
        else:
            # No limit - add all sitemap URLs
            for url in sitemap_urls_list:
                url_norm = normalize_url_for_storage(url)
                await frontier_seed(url_norm, base_domain, reset=False, db_path=crawl_db_path)
            print(f"Added {len(sitemap_urls_list)} URLs from sitemaps to frontier")

    processed = 0
    while True:
        # Check for shutdown request
        if shutdown_requested:
            print("Shutdown requested. Saving progress and exiting gracefully...")
            break
            
        # Check for URLs ready for retry first
        from .db import get_urls_ready_for_retry
        import aiosqlite
        try:
            async with aiosqlite.connect(crawl_db_path) as conn:
                retry_urls = await get_urls_ready_for_retry(conn, http_config.max_retries)
                if retry_urls:
                    print(f"Found {len(retry_urls)} URLs ready for retry")
                    # Add retry URLs back to frontier
                    for url_id, url in retry_urls:
                        await frontier_seed(url, base_domain, reset=False, db_path=crawl_db_path)
        except Exception as e:
            print(f"Error checking retry URLs: {e}")
            
        # Determine batch size based on whether there's a limit
        if limits.max_pages > 0:
            remaining = limits.max_pages - processed
            if remaining <= 0:
                break
            batch_size = min(cfg.max_concurrency, remaining)
        else:
            batch_size = cfg.max_concurrency
            
        batch = await frontier_next_batch(batch_size, db_path=crawl_db_path)
        if not batch:
            print("No more URLs in frontier - crawl complete!")
            break

        urls = [u for (u, _d, _p) in batch]
        depths = {u: d for (u, d, _p) in batch}
        parents = {u: p for (u, _d, p) in batch}
        
        # The frontier contains normalized URLs, but we need to fetch them
        # We'll use the normalized URLs directly since they should work for fetching
        # Use redirect tracking to capture redirect chains
        results = await fetch_many_with_redirect_tracking(urls, cfg)

        to_mark_done = []
        to_enqueue = []
        pages_to_write = []
        urls_to_upsert = []
        children_to_enqueue = []
        content_to_write = []
        links_to_write = []
        redirect_data_to_write = []

        for (status, final_url, headers, text, original, redirect_chain_json) in results:
            # Normalize URLs for storage
            original_norm = normalize_url_for_storage(original)
            final_norm = normalize_url_for_storage(final_url or original)
            
            # Look up depth and parent using the normalized URL (since frontier contains normalized URLs)
            depth = depths.get(original_norm, 0)
            parent_norm = parents.get(original_norm)
            k = classify(headers.get("Content-Type"), final_norm)
            
            # Normalize headers to save space
            headers_norm = normalize_headers(headers)
            
            # Log status code and URL
            print(f"[{status}] {original_norm} -> {final_norm} (depth: {depth}, type: {k})")
            
            # Check if this status code should be retried
            from .db import should_retry_status_code, record_failed_url, remove_failed_url, get_or_create_url_id
            if should_retry_status_code(status):
                # Record this URL for retry
                try:
                    url_id = await get_or_create_url_id(original_norm, base_domain, crawl_db_path)
                    
                    # Provide more descriptive failure reasons
                    if status == 0:
                        failure_reason = "Connection/timeout error"
                    elif status == 408:
                        failure_reason = "Request timeout (server slow)"
                    elif status == 423:
                        failure_reason = "Resource temporarily locked"
                    elif status == 429:
                        failure_reason = "Rate limited"
                    elif status == 420:
                        failure_reason = "Rate limited (Twitter)"
                    elif status == 451:
                        failure_reason = "Unavailable for legal reasons (geo-blocking?)"
                    elif 500 <= status < 600:
                        failure_reason = f"Server error {status}"
                    else:
                        failure_reason = f"HTTP {status}"
                    
                    async with aiosqlite.connect(crawl_db_path) as conn:
                        await record_failed_url(url_id, status, failure_reason, 
                                              conn, 
                                              http_config.retry_delay, 
                                              http_config.retry_backoff_factor)
                    print(f"  -> Marked for retry (status: {status})")
                except Exception as e:
                    print(f"  -> Error recording failed URL: {e}")
                
                # Don't mark as done - leave in frontier for retry
                continue
            else:
                # Success or permanent failure - mark as done
                to_mark_done.append(original_norm)
                
                # If successful, remove from failed_urls table
                if 200 <= status < 300:
                    try:
                        url_id = await get_or_create_url_id(original_norm, base_domain, crawl_db_path)
                        async with aiosqlite.connect(crawl_db_path) as conn:
                            await remove_failed_url(url_id, conn)
                    except Exception as e:
                        print(f"  -> Error removing from failed_urls: {e}")
            
            # Process redirect data if there was a redirect
            if redirect_chain_json and redirect_chain_json != "[]":
                import json
                try:
                    redirect_chain = json.loads(redirect_chain_json)
                    if len(redirect_chain) > 1:  # More than just the original request
                        chain_length = len(redirect_chain) - 1  # Exclude the original request
                        redirect_data_to_write.append((
                            original_norm,  # source_url
                            final_norm,     # target_url
                            redirect_chain_json,  # redirect_chain
                            chain_length,   # chain_length
                            status          # final_status
                        ))
                        print(f"  -> Redirect chain: {chain_length} redirects")
                except json.JSONDecodeError:
                    pass

            if k in {"sitemap", "sitemap_index"} or final_norm.lower().endswith(".xml"):
                urls_to_upsert.append((original_norm, k, base_domain, parent_norm or normalize_url_for_storage(start)))
                real_k, children = extract_from_sitemap(text)
                if real_k != k:
                    urls_to_upsert.append((original_norm, real_k, base_domain, parent_norm or normalize_url_for_storage(start)))
                if depth < limits.max_depth:
                    for child in children:
                        child_norm = normalize_url_for_storage(child)
                        
                        # Check if URL should be crawled based on classification
                        if should_crawl_url(child_norm, base_domain, allow_external, is_from_sitemap=True):
                            children_to_enqueue.append((child_norm, depth + 1, original_norm, base_domain))
                            print(f"  -> Enqueued from sitemap: {child_norm}")
                        else:
                            # Record but don't crawl
                            urls_to_upsert.append((child_norm, "other", base_domain, original_norm))
                            from .db import classify_url
                            classification = classify_url(child_norm, base_domain, is_from_sitemap=True)
                            print(f"  -> {classification.title()} URL from sitemap recorded: {child_norm}")
            elif k == "html":
                urls_to_upsert.append((original_norm, "html", base_domain, parent_norm or normalize_url_for_storage(start)))
                if text:
                    pages_to_write.append((original_norm, final_norm, status, headers_norm, text, base_domain))
                    
                    # Extract content from HTML
                    content_data = extract_content_from_html(text, headers)
                    if content_data['title'] or content_data['meta_description'] or content_data['h1_tags'] or content_data['h2_tags']:
                        # We'll need the URL ID, so we'll add this to content_to_write with a placeholder
                        # The actual URL ID will be resolved during batch processing
                        content_to_write.append((original_norm, content_data, base_domain))
                if depth < limits.max_depth and text:
                    # Extract links with metadata for internal links tracking
                    links, detailed_links = extract_links_with_metadata(text, final_norm)
                    print(f"  -> Found {len(links)} links in HTML")
                    
                    # Store detailed links data for internal links table
                    if detailed_links:
                        links_to_write.append((original_norm, detailed_links, base_domain))
                    
                    for child in links:
                        child_norm = normalize_url_for_storage(child)
                        
                        # Check if URL should be crawled based on classification
                        if should_crawl_url(child_norm, base_domain, allow_external, is_from_sitemap=False, user_agent=http_config.user_agent):
                            children_to_enqueue.append((child_norm, depth + 1, original_norm, base_domain))
                            print(f"  -> Enqueued: {child_norm}")
                        else:
                            # Record but don't crawl
                            urls_to_upsert.append((child_norm, "other", base_domain, original_norm))
                            from .db import classify_url
                            classification = classify_url(child_norm, base_domain, is_from_sitemap=False)
                            print(f"  -> {classification.title()} URL recorded: {child_norm}")
            else:
                urls_to_upsert.append((original_norm, k, base_domain, parent_norm or normalize_url_for_storage(start)))

        # Execute batch operations
        await frontier_mark_done(to_mark_done, base_domain, db_path=crawl_db_path)
        
        # Batch write pages
        if pages_to_write:
            print(f"  -> Writing {len(pages_to_write)} pages to database...")
            await batch_write_pages(pages_to_write, pages_db_path, crawl_db_path)
        
        # Batch upsert URLs
        if urls_to_upsert:
            print(f"  -> Upserting {len(urls_to_upsert)} URLs to database...")
            await batch_upsert_urls(urls_to_upsert, crawl_db_path)
        
        # Batch enqueue children
        if children_to_enqueue:
            print(f"  -> Enqueuing {len(children_to_enqueue)} children to frontier...")
            await batch_enqueue_frontier(children_to_enqueue, crawl_db_path)
        
        # Batch write content (after URLs are upserted so we can get URL IDs)
        if content_to_write:
            print(f"  -> Writing {len(content_to_write)} content extractions to database...")
            await batch_write_content_with_url_resolution(content_to_write, crawl_db_path)
        
        # Batch write internal links data (after URLs are upserted so we can get URL IDs)
        if links_to_write:
            print(f"  -> Writing {len(links_to_write)} internal links to database...")
            await batch_write_internal_links(links_to_write, crawl_db_path)
        
        # Batch write redirect data (after URLs are upserted so we can get URL IDs)
        if redirect_data_to_write:
            print(f"  -> Writing {len(redirect_data_to_write)} redirect chains to database...")
            await batch_write_redirects(redirect_data_to_write, crawl_db_path)
        
        processed += len(results)
        
        print(f"Batch complete: processed {len(results)} URLs, enqueued {len(children_to_enqueue)} new URLs")
        if limits.max_pages > 0:
            print(f"Total processed so far: {processed}/{limits.max_pages}")
        else:
            print(f"Total processed so far: {processed} (no limit)")
        print()

    q, d = await frontier_stats(db_path=crawl_db_path)
    print(f"Frontier status — queued: {q}, done: {d}")
    
    # Report retry statistics
    try:
        from .db import get_retry_statistics
        import aiosqlite
        async with aiosqlite.connect(crawl_db_path) as conn:
            stats = await get_retry_statistics(conn)
            
            if stats['total_failed'] > 0:
                print(f"\nRetry Statistics:")
                print(f"  Total failed URLs: {stats['total_failed']}")
                print(f"  Ready for retry: {stats['ready_for_retry']}")
                
                if stats['by_status']:
                    print(f"  By status code:")
                    for status, count in stats['by_status'].items():
                        status_name = {
                            0: "Connection/timeout",
                            408: "Request timeout", 
                            423: "Resource locked",
                            429: "Rate limited",
                            420: "Rate limited (Twitter)",
                            451: "Legal reasons",
                        }.get(status, f"HTTP {status}")
                        print(f"    {status} ({status_name}): {count} URLs")
                
                if stats['by_retry_count']:
                    print(f"  By retry attempts:")
                    for retry_count, count in stats['by_retry_count'].items():
                        print(f"    {retry_count} attempts: {count} URLs")
            else:
                print(f"\nNo failed URLs requiring retry.")
    except Exception as e:
        print(f"Error reporting retry statistics: {e}")
    
    if shutdown_requested:
        print("Crawl paused. Run the same command again to resume from where you left off.")
    else:
        print("Crawl completed successfully!")

if __name__ == "__main__":
    asyncio.run(crawl("https://example.com/", use_js=False, reset_frontier=True))
