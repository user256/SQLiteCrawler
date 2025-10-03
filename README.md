# SQLiteCrawler (persistent frontier)

**Databases**
- `pages.db`: raw HTML (compressed), status, headers, timestamps
- `crawl.db`: `urls` (normalized URL list + type) and `frontier` (persistent queue)

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .
# optional JS
pip install -e .[js]
playwright install

# fresh crawl on one host, depth 2, up to 1000 pages
python main.py https://example.com/ --max-depth 2 --max-pages 1000 --reset-frontier

# resume later (no reset needed)
python main.py https://example.com/ --max-pages 200

# allow offsite
python main.py https://example.com/ --offsite
```

### Environment overrides
- `SQLITECRAWLER_MAX_PAGES` (default 500)
- `SQLITECRAWLER_MAX_DEPTH` (default 3)
- `SQLITECRAWLER_SAME_HOST_ONLY` (default 1)

Frontier table columns: `url`, `depth`, `parent`, `status('queued'|'done')`, timestamps. The crawler marks processed items as `done` and enqueues new children respecting depth/host limits. You can safely stop and rerun — it picks up from the frontier.
