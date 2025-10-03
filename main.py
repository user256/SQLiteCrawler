import argparse, asyncio
from src.sqlitecrawler.crawl import crawl
from src.sqlitecrawler.config import CrawlLimits, HttpConfig, get_user_agent

if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description="Persistent async SQLite crawler with configurable user agents and crawling options",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s https://example.com
  %(prog)s https://example.com --js --max-pages 100
  %(prog)s https://example.com --user-agent chrome --offsite
  %(prog)s https://example.com --user-agent random --delay 0.5
  %(prog)s https://example.com/sitemap.xml --reset-frontier
        """
    )
    
    # Required arguments
    p.add_argument("start", help="Start URL or XML sitemap to begin crawling")
    
    # Crawling behavior
    p.add_argument("--js", action="store_true", 
                   help="Enable JavaScript rendering via Playwright (falls back to aiohttp if unavailable)")
    p.add_argument("--max-pages", type=int, default=None, 
                   help="Maximum pages to process in this run (default: no limit)")
    p.add_argument("--max-depth", type=int, default=None, 
                   help="Maximum crawl depth (default: 3)")
    p.add_argument("--offsite", action="store_true", 
                   help="Allow offsite traversal (default: same host only)")
    p.add_argument("--reset-frontier", action="store_true", 
                   help="Clear and reseed the frontier with the start URL")
    
    # User agent options
    p.add_argument("--user-agent", choices=["default", "chrome", "firefox", "safari", "edge", "mobile", "random"], 
                   default="default", help="User agent type to use (default: default)")
    p.add_argument("--custom-ua", type=str, 
                   help="Custom user agent string (overrides --user-agent)")
    
    # HTTP configuration
    p.add_argument("--timeout", type=int, default=None, 
                   help="Request timeout in seconds (default: 20)")
    p.add_argument("--concurrency", type=int, default=None, 
                   help="Maximum concurrent requests (default: 10)")
    p.add_argument("--delay", type=float, default=None, 
                   help="Delay between requests in seconds (default: 0.1)")
    p.add_argument("--ignore-robots", action="store_true",
                   help="Ignore robots.txt for crawlability (still use robots.txt to find sitemaps)")
    p.add_argument("--skip-robots-sitemaps", action="store_true",
                   help="Skip parsing robots.txt for sitemap discovery (only use common sitemap locations)")
    p.add_argument("--skip-sitemaps", action="store_true",
                   help="Skip all sitemap discovery and processing (crawl only the provided URL)")
    p.add_argument("--allow-external", action="store_true",
                   help="Allow crawling external URLs (default: internal only)")
    p.add_argument("--max-workers", type=int, default=2,
                   help="Maximum number of worker threads for database operations (default: 2)")
    
    # Output and logging
    p.add_argument("--verbose", "-v", action="store_true", 
                   help="Enable verbose output")
    p.add_argument("--quiet", "-q", action="store_true", 
                   help="Suppress non-error output")
    
    args = p.parse_args()

    # If skip-sitemaps is enabled, automatically enable skip-robots-sitemaps
    if args.skip_sitemaps:
        args.skip_robots_sitemaps = True

    # Create crawl limits
    limits = CrawlLimits(
        max_pages=args.max_pages if args.max_pages is not None else CrawlLimits().max_pages,
        max_depth=args.max_depth if args.max_depth is not None else CrawlLimits().max_depth,
        same_host_only=not args.offsite,
    )
    
    # Create HTTP configuration
    user_agent = args.custom_ua if args.custom_ua else get_user_agent(args.user_agent)
    http_config = HttpConfig(
        user_agent=user_agent,
        timeout=args.timeout if args.timeout is not None else HttpConfig().timeout,
        max_concurrency=args.concurrency if args.concurrency is not None else HttpConfig().max_concurrency,
        delay_between_requests=args.delay if args.delay is not None else HttpConfig().delay_between_requests,
        respect_robots_txt=not args.ignore_robots,
        ignore_robots_crawlability=args.ignore_robots,
        skip_robots_sitemaps=args.skip_robots_sitemaps,
        skip_sitemaps=args.skip_sitemaps,
    )
    
    # Print configuration if verbose
    if args.verbose:
        print(f"Starting crawl with configuration:")
        print(f"  Start URL: {args.start}")
        print(f"  User Agent: {http_config.user_agent}")
        print(f"  Max Pages: {limits.max_pages}")
        print(f"  Max Depth: {limits.max_depth}")
        print(f"  Same Host Only: {limits.same_host_only}")
        print(f"  JavaScript Rendering: {args.js}")
        print(f"  Timeout: {http_config.timeout}s")
        print(f"  Concurrency: {http_config.max_concurrency}")
        print(f"  Delay: {http_config.delay_between_requests}s")
        print(f"  Respect robots.txt: {http_config.respect_robots_txt}")
        print(f"  Ignore robots for crawlability: {http_config.ignore_robots_crawlability}")
        print(f"  Skip robots.txt sitemaps: {http_config.skip_robots_sitemaps}")
        print(f"  Skip sitemaps: {http_config.skip_sitemaps}")
        print(f"  Allow external URLs: {args.allow_external}")
        print(f"  Max workers: {args.max_workers}")
        print()

    asyncio.run(crawl(args.start, use_js=args.js, limits=limits, reset_frontier=args.reset_frontier, http_config=http_config, allow_external=args.allow_external, max_workers=args.max_workers, verbose=args.verbose))
