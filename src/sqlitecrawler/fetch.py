from __future__ import annotations
import asyncio
import aiohttp
import json
from typing import Dict, Tuple, List
from .config import HttpConfig

async def fetch(url: str, cfg: HttpConfig) -> Tuple[int, str, Dict[str, str], str, str]:
    """Return (status, final_url, headers, text, url) for a single request."""
    timeout = aiohttp.ClientTimeout(total=cfg.timeout)
    async with aiohttp.ClientSession(headers={"User-Agent": cfg.user_agent}, timeout=timeout) as session:
        try:
            async with session.get(url, allow_redirects=True) as resp:
                text = await resp.text(errors="ignore")
                return resp.status, str(resp.url), dict(resp.headers), text, url
        except Exception:
            return 0, url, {}, "", url

async def fetch_with_redirect_tracking(url: str, cfg: HttpConfig) -> Tuple[int, str, Dict[str, str], str, str, str]:
    """Return (status, final_url, headers, text, url, redirect_chain_json) for a single request with redirect tracking."""
    timeout = aiohttp.ClientTimeout(total=cfg.timeout)
    redirect_chain = []
    
    async with aiohttp.ClientSession(headers={"User-Agent": cfg.user_agent}, timeout=timeout) as session:
        try:
            current_url = url
            max_redirects = 10  # Prevent infinite redirects
            
            for _ in range(max_redirects):
                async with session.get(current_url, allow_redirects=False) as resp:
                    # Record this step in the redirect chain
                    redirect_chain.append({
                        "url": current_url,
                        "status": resp.status,
                        "headers": dict(resp.headers)
                    })
                    
                    # If it's a redirect, follow it
                    if resp.status in (301, 302, 303, 307, 308):
                        location = resp.headers.get('location')
                        if location:
                            # Handle relative URLs
                            if location.startswith('/'):
                                from urllib.parse import urljoin
                                current_url = urljoin(current_url, location)
                            elif not location.startswith(('http://', 'https://')):
                                from urllib.parse import urljoin
                                current_url = urljoin(current_url, location)
                            else:
                                current_url = location
                            continue
                    
                    # Not a redirect, we're done
                    text = await resp.text(errors="ignore")
                    return resp.status, str(resp.url), dict(resp.headers), text, url, json.dumps(redirect_chain)
            
            # If we hit max redirects, return the last response
            if redirect_chain:
                last_step = redirect_chain[-1]
                return last_step["status"], current_url, last_step["headers"], "", url, json.dumps(redirect_chain)
            else:
                return 0, url, {}, "", url, json.dumps([])
                
        except Exception as e:
            return 0, url, {}, "", url, json.dumps(redirect_chain)

# ---- JS rendering path via Playwright ----
# Usage: pip install .[js] && playwright install
async def fetch_js(url: str, cfg: HttpConfig) -> Tuple[int, str, Dict[str, str], str, str]:
    try:
        from playwright.async_api import async_playwright
    except Exception:
        # Fallback to plain fetch if Playwright isn't available
        return await fetch(url, cfg)

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent=cfg.user_agent)
            page = await context.new_page()
            try:
                resp = await page.goto(url, timeout=cfg.timeout * 1000, wait_until="networkidle")
                html = await page.content()
                status = resp.status if resp else 0
                final_url = page.url
                headers = dict(resp.headers()) if resp else {}
                return status, final_url, headers, html, url
            finally:
                await context.close()
                await browser.close()
    except Exception:
        return 0, url, {}, "", url

async def fetch_many(urls: list[str], cfg: HttpConfig, use_js: bool = False):
    sem = asyncio.Semaphore(cfg.max_concurrency)
    results = []

    async def _task(u: str):
        async with sem:
            return await (fetch_js(u, cfg) if use_js else fetch(u, cfg))

    tasks = [_task(u) for u in urls]
    for coro in asyncio.as_completed(tasks):
        results.append(await coro)
    return results

async def fetch_many_with_redirect_tracking(urls: list[str], cfg: HttpConfig):
    """Fetch multiple URLs with redirect tracking."""
    sem = asyncio.Semaphore(cfg.max_concurrency)
    results = []

    async def _task(u: str):
        async with sem:
            return await fetch_with_redirect_tracking(u, cfg)

    tasks = [_task(u) for u in urls]
    for coro in asyncio.as_completed(tasks):
        results.append(await coro)
    return results
