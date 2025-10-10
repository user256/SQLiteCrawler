import argparse, asyncio, os, sys
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
    p.add_argument("start", nargs='?', help="Start URL or XML sitemap to begin crawling (optional when using --csv-file)")
    
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
    
    # Retry configuration
    p.add_argument("--max-retries", type=int, default=3,
                   help="Maximum number of retry attempts for failed URLs (default: 3)")
    p.add_argument("--retry-delay", type=float, default=1.0,
                   help="Initial delay between retries in seconds (default: 1.0)")
    p.add_argument("--retry-backoff", type=float, default=2.0,
                   help="Backoff factor for retry delays (default: 2.0)")
    
    # Authentication configuration
    p.add_argument("--auth-username", type=str, default="",
                   help="Username for HTTP authentication (basic/digest)")
    p.add_argument("--auth-password", type=str, default="",
                   help="Password for HTTP authentication (basic/digest)")
    p.add_argument("--auth-type", type=str, choices=["basic", "digest", "bearer", "jwt", "api_key", "custom"], default="basic",
                   help="Authentication type: basic, digest, bearer, jwt, api_key, or custom (default: basic)")
    p.add_argument("--auth-domain", type=str, default="",
                   help="Restrict authentication to specific domain (optional)")
    p.add_argument("--auth-token", type=str, default="",
                   help="Token for bearer/jwt/api_key authentication")
    p.add_argument("--auth-header", type=str, default="",
                   help="Custom header name for api_key authentication (default: X-API-Key)")
    p.add_argument("--auth-custom-headers", type=str, default="",
                   help="Custom headers in format 'Header1:Value1,Header2:Value2'")
    
    # HTTP/2 and compression
    p.add_argument("--no-http2", action="store_true",
                   help="Disable HTTP/2 support (use HTTP/1.1)")
    p.add_argument("--no-brotli", action="store_true",
                   help="Disable Brotli compression support")
    
    # Politeness and rate limiting
    p.add_argument("--no-adaptive-delay", action="store_true",
                   help="Disable adaptive delay (use fixed delay only)")
    p.add_argument("--min-delay", type=float, default=None,
                   help="Minimum delay between requests in seconds (default: 0.1)")
    p.add_argument("--max-delay", type=float, default=None,
                   help="Maximum delay between requests in seconds (default: 10.0)")
    p.add_argument("--delay-increase", type=float, default=None,
                   help="Factor to increase delay on errors (default: 1.5)")
    p.add_argument("--delay-decrease", type=float, default=None,
                   help="Factor to decrease delay on success (default: 0.9)")
    
    # Conditional requests
    p.add_argument("--no-conditional-requests", action="store_true",
                   help="Disable conditional requests (ETag/If-Modified-Since)")
    
    # CSV crawl support
    p.add_argument("--csv-file", type=str, default="",
                   help="CSV file containing URLs to crawl (one URL per line or column)")
    p.add_argument("--csv-seed", action="store_true",
                   help="Treat CSV URLs as seed URLs - also crawl sitemaps, robots.txt, and follow internal links")
    p.add_argument("--csv-column", type=str, default="url",
                   help="Column name containing URLs in CSV file (default: 'url')")
    
    # Crawl comparison support
    p.add_argument("--compare-domain", type=str, default="",
                   help="Domain to compare against (e.g., https://staging.example.com)")
    p.add_argument("--commercial-csv", type=str, default="",
                   help="CSV file containing commercial page URLs for comparison analysis")
    p.add_argument("--compare-links", action="store_true",
                   help="Enable detailed link comparison analysis (added/lost internal links)")
    
    # Output and logging
    p.add_argument("--verbose", "-v", action="store_true", 
                   help="Enable verbose output")
    p.add_argument("--quiet", "-q", action="store_true", 
                   help="Suppress non-error output")
    
    args = p.parse_args()

    # Validate CSV arguments
    if args.csv_file and not os.path.exists(args.csv_file):
        print(f"Error: CSV file not found: {args.csv_file}")
        sys.exit(1)
    
    if args.csv_file and not args.start:
        # CSV mode - start URL argument is optional
        pass
    elif not args.csv_file and not args.start:
        print("Error: Either start URL or --csv-file must be specified")
        sys.exit(1)

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
    
    # Create authentication configuration if provided
    auth_config = None
    if (args.auth_username and args.auth_password) or args.auth_token or args.auth_custom_headers:
        from src.sqlitecrawler.config import AuthConfig
        
        # Parse custom headers
        custom_headers = {}
        if args.auth_custom_headers:
            for header_pair in args.auth_custom_headers.split(','):
                if ':' in header_pair:
                    header_name, header_value = header_pair.split(':', 1)
                    custom_headers[header_name.strip()] = header_value.strip()
        
        auth_config = AuthConfig(
            username=args.auth_username,
            password=args.auth_password,
            auth_type=args.auth_type,
            domain=args.auth_domain,
            token=args.auth_token,
            custom_headers=custom_headers if custom_headers else None
        )
        
        # Set custom API key header if provided
        if args.auth_type == "api_key" and args.auth_header:
            auth_config.api_key_header = args.auth_header
    
    # Process CSV file if provided
    csv_urls = []
    if args.csv_file:
        try:
            import csv
            with open(args.csv_file, 'r', newline='', encoding='utf-8') as csvfile:
                # Try to detect if it's a proper CSV with headers
                sample = csvfile.read(1024)
                csvfile.seek(0)
                
                # Check if the first line looks like headers
                has_headers = ',' in sample and any(char.isalpha() for char in sample.split('\n')[0])
                
                if has_headers:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        if args.csv_column in row and row[args.csv_column]:
                            csv_urls.append(row[args.csv_column].strip())
                else:
                    # Treat as simple list of URLs, one per line
                    for line in csvfile:
                        url = line.strip()
                        if url and not url.startswith('#'):  # Skip empty lines and comments
                            csv_urls.append(url)
            
            if not csv_urls:
                print(f"Error: No URLs found in CSV file: {args.csv_file}")
                sys.exit(1)
                
            print(f"Loaded {len(csv_urls)} URLs from CSV file: {args.csv_file}")
            if args.csv_seed:
                print("CSV seed mode: Will also crawl sitemaps, robots.txt, and follow internal links")
            else:
                print("CSV restricted mode: Will only crawl URLs from the CSV file")
                
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            sys.exit(1)
    
    http_config = HttpConfig(
        user_agent=user_agent,
        timeout=args.timeout if args.timeout is not None else HttpConfig().timeout,
        max_concurrency=args.concurrency if args.concurrency is not None else HttpConfig().max_concurrency,
        delay_between_requests=args.delay if args.delay is not None else HttpConfig().delay_between_requests,
        respect_robots_txt=not args.ignore_robots,
        ignore_robots_crawlability=args.ignore_robots,
        skip_robots_sitemaps=args.skip_robots_sitemaps,
        skip_sitemaps=args.skip_sitemaps,
        enable_http2=not args.no_http2,
        enable_brotli=not args.no_brotli,
        max_retries=args.max_retries,
        retry_delay=args.retry_delay,
        retry_backoff_factor=args.retry_backoff,
        enable_adaptive_delay=not args.no_adaptive_delay,
        min_delay=args.min_delay if args.min_delay is not None else HttpConfig().min_delay,
        max_delay=args.max_delay if args.max_delay is not None else HttpConfig().max_delay,
        delay_increase_factor=args.delay_increase if args.delay_increase is not None else HttpConfig().delay_increase_factor,
        delay_decrease_factor=args.delay_decrease if args.delay_decrease is not None else HttpConfig().delay_decrease_factor,
        enable_conditional_requests=not args.no_conditional_requests,
        auth=auth_config,
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
        print(f"  Max Retries: {http_config.max_retries}")
        print(f"  Retry Delay: {http_config.retry_delay}s")
        print(f"  Retry Backoff: {http_config.retry_backoff_factor}x")
        print(f"  Respect robots.txt: {http_config.respect_robots_txt}")
        print(f"  Ignore robots for crawlability: {http_config.ignore_robots_crawlability}")
        print(f"  Skip robots.txt sitemaps: {http_config.skip_robots_sitemaps}")
        print(f"  Skip sitemaps: {http_config.skip_sitemaps}")
        print(f"  HTTP/2 Support: {http_config.enable_http2}")
        print(f"  Brotli Compression: {http_config.enable_brotli}")
        print(f"  Adaptive Delay: {http_config.enable_adaptive_delay}")
        if http_config.enable_adaptive_delay:
            print(f"  Min Delay: {http_config.min_delay}s")
            print(f"  Max Delay: {http_config.max_delay}s")
            print(f"  Delay Increase Factor: {http_config.delay_increase_factor}x")
            print(f"  Delay Decrease Factor: {http_config.delay_decrease_factor}x")
        print(f"  Conditional Requests: {http_config.enable_conditional_requests}")
        print(f"  Allow external URLs: {args.allow_external}")
        print(f"  Max workers: {args.max_workers}")
        if auth_config:
            print(f"  Authentication: {auth_config.auth_type} (user: {auth_config.username})")
            if auth_config.domain:
                print(f"  Auth Domain: {auth_config.domain}")
        else:
            print(f"  Authentication: None")
        print()

    # Use first CSV URL as start URL if no start URL provided
    start_url = args.start
    if not start_url and csv_urls:
        start_url = csv_urls[0]
        print(f"Using first CSV URL as start URL: {start_url}")
    
    # Handle crawl comparison if compare-domain is specified
    if args.compare_domain:
        from src.sqlitecrawler.comparison import run_crawl_comparison
        asyncio.run(run_crawl_comparison(
            origin_url=start_url,
            staging_url=args.compare_domain,
            commercial_csv=args.commercial_csv,
            compare_links=args.compare_links,
            use_js=args.js,
            limits=limits,
            http_config=http_config,
            allow_external=args.allow_external,
            max_workers=args.max_workers,
            verbose=args.verbose
        ))
    else:
        asyncio.run(crawl(start_url, use_js=args.js, limits=limits, reset_frontier=args.reset_frontier, http_config=http_config, allow_external=args.allow_external, max_workers=args.max_workers, verbose=args.verbose, csv_urls=csv_urls, csv_seed_mode=args.csv_seed))
