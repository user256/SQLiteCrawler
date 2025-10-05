"""
Enhanced HTTP client with HTTP/2 and Brotli support.
"""
from __future__ import annotations
import asyncio
import httpx
import brotli
import json
from typing import Dict, Tuple, List, Optional
from urllib.parse import urlparse
from .config import HttpConfig, AuthConfig


def _should_use_auth(url: str, auth: AuthConfig) -> bool:
    """Check if authentication should be used for this URL."""
    if not auth or not auth.username or not auth.password:
        return False
    
    # If domain is specified, only use auth for that domain
    if auth.domain:
        parsed_url = urlparse(url)
        return parsed_url.netloc.lower() == auth.domain.lower()
    
    return True


def _create_auth(auth: AuthConfig) -> Optional[Tuple[str, str]]:
    """Create httpx authentication tuple for basic/digest auth."""
    if auth.auth_type in ["basic", "digest"] and auth.username and auth.password:
        return (auth.username, auth.password)
    return None


def _get_auth_headers(auth: AuthConfig) -> Dict[str, str]:
    """Get authentication headers for token-based and custom authentication."""
    headers = {}
    
    if not auth:
        return headers
    
    # Token-based authentication
    if auth.auth_type == "bearer" and auth.token:
        headers["Authorization"] = f"Bearer {auth.token}"
    elif auth.auth_type == "jwt" and auth.token:
        headers["Authorization"] = f"JWT {auth.token}"
    elif auth.auth_type == "api_key" and auth.token:
        # Default to X-API-Key header, but allow custom header name
        header_name = getattr(auth, 'api_key_header', 'X-API-Key')
        headers[header_name] = auth.token
    
    # Custom headers
    if auth.custom_headers:
        headers.update(auth.custom_headers)
    
    return headers


def _get_compression_headers() -> Dict[str, str]:
    """Get headers for compression support."""
    return {
        "Accept-Encoding": "gzip, deflate, br",  # br = Brotli
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    }


def _decompress_content(content: bytes, encoding: str) -> bytes:
    """Decompress content based on encoding."""
    if encoding == "br":
        return brotli.decompress(content)
    elif encoding == "gzip":
        import gzip
        return gzip.decompress(content)
    elif encoding == "deflate":
        import zlib
        return zlib.decompress(content)
    else:
        return content


async def fetch(url: str, cfg: HttpConfig, conditional_headers: Dict[str, str] = None) -> Tuple[int, str, Dict[str, str], str, str]:
    """Return (status, final_url, headers, text, url) for a single request with HTTP/2 and Brotli support."""
    
    # Prepare authentication if needed
    auth = None
    if _should_use_auth(url, cfg.auth):
        auth = _create_auth(cfg.auth)
    
    # Prepare headers
    headers = {
        "User-Agent": cfg.user_agent,
        **_get_compression_headers(),
        **_get_auth_headers(cfg.auth)
    }
    
    # Add conditional headers if provided
    if conditional_headers:
        headers.update(conditional_headers)
    
    # Create HTTP/2 client with timeout
    timeout = httpx.Timeout(cfg.timeout)
    
    async with httpx.AsyncClient(
        http2=True,  # Enable HTTP/2
        timeout=timeout,
        auth=auth,
        headers=headers,
        follow_redirects=True
    ) as client:
        try:
            response = await client.get(url)
            
            # Get content encoding
            content_encoding = response.headers.get("content-encoding", "").lower()
            
            # Decompress content if needed
            content = response.content
            if content_encoding:
                try:
                    content = _decompress_content(content, content_encoding)
                except Exception:
                    # If decompression fails, use original content
                    pass
            
            # Convert to text
            text = content.decode("utf-8", errors="ignore")
            
            return response.status_code, str(response.url), dict(response.headers), text, url
            
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return 0, url, {}, "", url


async def fetch_with_redirect_tracking(url: str, cfg: HttpConfig, conditional_headers: Dict[str, str] = None) -> Tuple[int, str, Dict[str, str], str, str, str]:
    """Return (status, final_url, headers, text, url, redirect_chain_json) for a single request with redirect tracking."""
    
    # Prepare authentication if needed
    auth = None
    if _should_use_auth(url, cfg.auth):
        auth = _create_auth(cfg.auth)
    
    # Prepare headers
    headers = {
        "User-Agent": cfg.user_agent,
        **_get_compression_headers(),
        **_get_auth_headers(cfg.auth)
    }
    
    # Add conditional headers if provided
    if conditional_headers:
        headers.update(conditional_headers)
    
    # Create HTTP/2 client with timeout
    timeout = httpx.Timeout(cfg.timeout)
    
    async with httpx.AsyncClient(
        http2=True,  # Enable HTTP/2
        timeout=timeout,
        auth=auth,
        headers=headers,
        follow_redirects=False  # We'll handle redirects manually to track them
    ) as client:
        try:
            redirect_chain = []
            current_url = url
            
            while len(redirect_chain) < 10:  # Prevent infinite redirects
                response = await client.get(current_url)
                
                # Add to redirect chain
                redirect_chain.append({
                    "url": current_url,
                    "status": response.status_code,
                    "headers": dict(response.headers)
                })
                
                # Check if it's a redirect
                if response.status_code in [301, 302, 303, 307, 308]:
                    location = response.headers.get("location")
                    if location:
                        # Handle relative URLs
                        if location.startswith("/"):
                            from urllib.parse import urljoin
                            current_url = urljoin(current_url, location)
                        else:
                            current_url = location
                        continue
                
                # Not a redirect, we're done
                break
            
            # Get content encoding
            content_encoding = response.headers.get("content-encoding", "").lower()
            
            # Decompress content if needed
            content = response.content
            if content_encoding:
                try:
                    content = _decompress_content(content, content_encoding)
                except Exception:
                    # If decompression fails, use original content
                    pass
            
            # Convert to text
            text = content.decode("utf-8", errors="ignore")
            
            return response.status_code, str(response.url), dict(response.headers), text, url, json.dumps(redirect_chain)
            
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return 0, url, {}, "", url, json.dumps([])


async def fetch_batch(urls: List[str], cfg: HttpConfig, max_concurrency: int = 5) -> List[Tuple[int, str, Dict[str, str], str, str]]:
    """Fetch multiple URLs concurrently with HTTP/2 and Brotli support."""
    
    # Prepare authentication if needed
    auth = None
    if cfg.auth and cfg.auth.username and cfg.auth.password:
        auth = _create_auth(cfg.auth)
    
    # Prepare headers
    headers = {
        "User-Agent": cfg.user_agent,
        **_get_compression_headers(),
        **_get_auth_headers(cfg.auth)
    }
    
    # Create HTTP/2 client with timeout
    timeout = httpx.Timeout(cfg.timeout)
    
    async with httpx.AsyncClient(
        http2=True,  # Enable HTTP/2
        timeout=timeout,
        auth=auth,
        headers=headers,
        follow_redirects=True,
        limits=httpx.Limits(max_connections=max_concurrency)
    ) as client:
        
        async def fetch_single(url: str) -> Tuple[int, str, Dict[str, str], str, str]:
            try:
                # Check if authentication should be used for this specific URL
                if not _should_use_auth(url, cfg.auth):
                    # Create headers without auth for this URL
                    no_auth_headers = {
                        "User-Agent": cfg.user_agent,
                        **_get_compression_headers()
                    }
                    # Create a new client without auth for this URL
                    async with httpx.AsyncClient(
                        http2=True,
                        timeout=timeout,
                        headers=no_auth_headers,
                        follow_redirects=True
                    ) as no_auth_client:
                        response = await no_auth_client.get(url)
                else:
                    response = await client.get(url)
                
                # Get content encoding
                content_encoding = response.headers.get("content-encoding", "").lower()
                
                # Decompress content if needed
                content = response.content
                if content_encoding:
                    try:
                        content = _decompress_content(content, content_encoding)
                    except Exception:
                        # If decompression fails, use original content
                        pass
                
                # Convert to text
                text = content.decode("utf-8", errors="ignore")
                
                return response.status_code, str(response.url), dict(response.headers), text, url
                
            except Exception as e:
                print(f"Error fetching {url}: {e}")
                return 0, url, {}, "", url
        
        # Execute requests concurrently
        tasks = [fetch_single(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle any exceptions
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"Exception for {urls[i]}: {result}")
                processed_results.append((0, urls[i], {}, "", urls[i]))
            else:
                processed_results.append(result)
        
        return processed_results


def get_http_version_info() -> Dict[str, str]:
    """Get information about HTTP client capabilities."""
    return {
        "http_client": "httpx",
        "http2_support": "enabled",
        "brotli_support": "enabled",
        "compression_formats": "gzip, deflate, br (brotli)"
    }
