"""Haver Analytics MCP Server.

Token-efficient tools for discovering and fetching data from Haver Analytics
across all 138 databases (US + international). Uses a local SQLite FTS5
metadata database for fast keyword search on US databases when the
HAVER_METADATA_DB environment variable points at one; otherwise falls back
to the Haver REST API for everything.
"""

import json
import os
import sqlite3
from pathlib import Path

import requests
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("haver")

# Optional local metadata DB. Set HAVER_METADATA_DB to a prebuilt SQLite
# file (with `databases` and `series` tables, plus an optional `series_fts`
# FTS5 index) to enable fast local keyword search on US databases. When the
# env var is unset or the file is missing, all queries route through the
# Haver REST API.
_metadata_env = os.environ.get("HAVER_METADATA_DB")
METADATA_DB: Path | None = Path(_metadata_env) if _metadata_env else None

# Haver API base URL
_HAVER_URL = "https://api.haverview.com"

# Cached set of databases available in local SQLite
_LOCAL_DATABASES: set | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _has_local_db() -> bool:
    """True if a local metadata DB is configured and present on disk."""
    return METADATA_DB is not None and METADATA_DB.exists()


def _truncate(text: str, max_len: int = 80) -> str:
    """Truncate text to max_len chars, appending '...' if needed."""
    if not text or len(text) <= max_len:
        return (text or "").strip()
    return text[: max_len - 3].strip() + "..."


def _compact(obj) -> str:
    """Compact JSON serialisation — no whitespace."""
    return json.dumps(obj, separators=(",", ":"), default=str)


def _is_local_db(database: str) -> bool:
    """Check whether a database is in the local SQLite metadata."""
    global _LOCAL_DATABASES
    if _LOCAL_DATABASES is None:
        if not _has_local_db():
            _LOCAL_DATABASES = set()
        else:
            conn = sqlite3.connect(str(METADATA_DB))
            _LOCAL_DATABASES = {
                r[0] for r in conn.execute("SELECT code FROM databases").fetchall()
            }
            conn.close()
    return database.upper() in _LOCAL_DATABASES


def _api_headers() -> dict:
    """Return auth headers for the Haver REST API."""
    api_key = os.environ.get("HAVER_API_KEY")
    if not api_key:
        raise ValueError("HAVER_API_KEY environment variable not set")
    return {"Content-Type": "application/json", "X-API-Key": api_key}


def _get_client():
    """Create authenticated Haver client (for data fetching)."""
    from haver import Haver

    api_key = os.environ.get("HAVER_API_KEY")
    if not api_key:
        raise ValueError("HAVER_API_KEY environment variable not set")
    return Haver(api_key=api_key)


def _freq_abbrev(freq) -> str:
    """Convert frequency to short abbreviation."""
    if not freq:
        return ""
    f = str(freq).strip().lower()
    mapping = {
        "annual": "A", "quarterly": "Q", "monthly": "M",
        "weekly": "W", "daily": "D", "semiannual": "SA",
    }
    for key, val in mapping.items():
        if key in f:
            return val
    # Haver API uses numeric codes
    num_map = {"10": "A", "20": "Q", "30": "M", "40": "M", "50": "W", "60": "D"}
    return num_map.get(f, f[:3])


# ---------------------------------------------------------------------------
# Tool 1: haver_search
# ---------------------------------------------------------------------------

@mcp.tool()
def haver_search(query: str, database: str = "", limit: int = 25) -> str:
    """Search for Haver series by keyword across all 138 databases.

    Uses fast local FTS5 search for 21 US databases, falls back to the
    Haver REST API for international databases. Returns compact results
    with truncated descriptions.

    Args:
        query: Search terms (e.g. 'TTF natural gas', 'GDP contribution')
        database: Optional database filter (e.g. 'USECON', 'GERMANY')
        limit: Max results to return (default 25, max 100)
    """
    limit = min(max(1, limit), 100)
    db_upper = database.upper().strip() if database else ""

    # Route 1: local FTS5 search (fast path for US databases)
    if db_upper and _is_local_db(db_upper):
        return _search_local_fts(query, db_upper, limit)

    if not db_upper and _has_local_db():
        # Search local DBs first, supplement with API if few results
        local = _search_local_fts(query, "", limit)
        local_obj = json.loads(local)
        if local_obj.get("total", 0) >= limit:
            return local
        # Supplement with API results
        api_results = _search_api(query, "", limit)
        api_obj = json.loads(api_results)
        # Merge, dedup by code
        seen = {r["code"] for r in local_obj["results"]}
        for r in api_obj.get("results", []):
            if r["code"] not in seen and len(local_obj["results"]) < limit:
                local_obj["results"].append(r)
                seen.add(r["code"])
        local_obj["total"] = len(local_obj["results"])
        return _compact(local_obj)

    # Route 2: API search (international or no local DB)
    return _search_api(query, db_upper, limit)


def _search_local_fts(query: str, database: str, limit: int) -> str:
    """FTS5 search on local SQLite metadata."""
    if not _has_local_db():
        return _compact({"total": 0, "results": [], "error": "Local metadata DB not found"})

    conn = sqlite3.connect(str(METADATA_DB))
    conn.row_factory = sqlite3.Row

    # Check for FTS table
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]

    if "series_fts" in tables:
        fts_query = " ".join(f'"{w}"' for w in query.split())
        sql = """
            SELECT s.series_code, s.description, s.database, s.frequency,
                   s.start_date, s.end_date, s.source
            FROM series_fts fts
            JOIN series s ON s.id = fts.rowid
            WHERE series_fts MATCH ?
        """
        params: list = [fts_query]
    else:
        sql = """
            SELECT series_code, description, database, frequency,
                   start_date, end_date, source
            FROM series
            WHERE (series_code LIKE ? OR description LIKE ?)
        """
        like = f"%{query}%"
        params = [like, like]

    if database:
        sql += " AND s.database = ?" if "series_fts" in tables else " AND database = ?"
        params.append(database)

    sql += f" LIMIT {limit}"

    try:
        rows = conn.execute(sql, params).fetchall()
    except Exception:
        # FTS5 syntax error — fall back to LIKE
        sql2 = """
            SELECT series_code, description, database, frequency,
                   start_date, end_date, source
            FROM series
            WHERE (series_code LIKE ? OR description LIKE ?)
        """
        like = f"%{query}%"
        params2: list = [like, like]
        if database:
            sql2 += " AND database = ?"
            params2.append(database)
        sql2 += f" LIMIT {limit}"
        rows = conn.execute(sql2, params2).fetchall()

    conn.close()

    results = []
    for r in rows:
        start = (r["start_date"] or "").strip()
        end = (r["end_date"] or "").strip()
        rng = f"{start} to {end}" if start else ""
        results.append({
            "code": f"{r['series_code']}@{r['database']}",
            "desc": _truncate(r["description"]),
            "freq": _freq_abbrev(r["frequency"]),
            "range": rng,
        })

    return _compact({"total": len(results), "results": results})


def _search_api(query: str, database: str, limit: int) -> str:
    """Search via Haver REST API (works for all 138 databases)."""
    try:
        r = requests.get(
            f"{_HAVER_URL}/v4/search",
            params={"query": query},
            headers=_api_headers(),
            timeout=15,
        )
        r.raise_for_status()
        data = r.json().get("data", [])
    except Exception as e:
        return _compact({"total": 0, "results": [], "error": str(e)})

    results = []
    for item in data:
        db_name = item.get("db_name", "")
        if database and db_name.upper() != database:
            continue
        results.append({
            "code": f"{item['name']}@{db_name}",
            "desc": _truncate(item.get("description", "")),
            "db": db_name,
        })
        if len(results) >= limit:
            break

    return _compact({"total": len(results), "results": results})


# ---------------------------------------------------------------------------
# Tool 2: haver_search_by_code
# ---------------------------------------------------------------------------

@mcp.tool()
def haver_search_by_code(pattern: str, database: str = "", limit: int = 25) -> str:
    """Find Haver series by code pattern.

    Use SQL-style wildcards: % matches any string, _ matches one character.
    Best for navigating Haver's code conventions (e.g. R134G% for all
    Germany yield series in INTDAILY, PAT% for GDP contributions in USECON).

    Args:
        pattern: Code pattern with wildcards (e.g. 'R134G%', 'PAT%', '%CPI%')
        database: Database to search (strongly recommended for non-US databases)
        limit: Max results (default 25, max 100)
    """
    limit = min(max(1, limit), 100)
    db_upper = database.upper().strip() if database else ""
    pattern_upper = pattern.upper().strip()

    # Route 1: local SQLite
    if (db_upper and _is_local_db(db_upper)) or (not db_upper and _has_local_db()):
        return _search_by_code_local(pattern_upper, db_upper, limit)

    # Route 2: API — use get_series with prefix cursor
    if db_upper:
        return _search_by_code_api(pattern_upper, db_upper, limit)

    return _compact({"total": 0, "results": [], "error": "Specify a database for non-US code search"})


def _search_by_code_local(pattern: str, database: str, limit: int) -> str:
    """Code pattern search on local SQLite."""
    conn = sqlite3.connect(str(METADATA_DB))
    conn.row_factory = sqlite3.Row

    sql = "SELECT series_code, description, database, frequency FROM series WHERE series_code LIKE ?"
    params: list = [pattern]
    if database:
        sql += " AND database = ?"
        params.append(database)
    sql += f" ORDER BY series_code LIMIT {limit}"

    rows = conn.execute(sql, params).fetchall()
    conn.close()

    results = []
    for r in rows:
        results.append({
            "code": f"{r['series_code']}@{r['database']}",
            "desc": _truncate(r["description"]),
            "freq": _freq_abbrev(r["frequency"]),
        })

    return _compact({"total": len(results), "results": results})


def _search_by_code_api(pattern: str, database: str, limit: int) -> str:
    """Code pattern search via API. Uses prefix from pattern as cursor."""
    # Extract the prefix before the first wildcard
    prefix = pattern.split("%")[0].split("_")[0]
    if not prefix:
        return _compact({"total": 0, "results": [], "error": "Pattern must start with a non-wildcard prefix"})

    # The API's `page` param is exclusive — it returns series AFTER the cursor.
    # Step back by one character so the page includes the prefix itself.
    cursor = prefix[:-1] + chr(ord(prefix[-1]) - 1) if prefix else ""

    try:
        url = f"{_HAVER_URL}/v4/database/{database}/series"
        params = {"per_page": 1000}
        if cursor:
            params["page"] = cursor
        r = requests.get(url, params=params, headers=_api_headers(), timeout=30)
        r.raise_for_status()
        series = r.json().get("data", [])
    except Exception as e:
        return _compact({"total": 0, "results": [], "error": str(e)})

    # Filter by pattern (convert SQL wildcards to simple matching)
    import fnmatch
    glob_pattern = pattern.replace("%", "*").replace("_", "?")

    results = []
    for item in series:
        code = item.get("name", "")
        if fnmatch.fnmatch(code.upper(), glob_pattern):
            results.append({
                "code": f"{code}@{database}",
                "desc": _truncate(item.get("description", "")),
                "freq": _freq_abbrev(item.get("originalFrequency", "")),
            })
            if len(results) >= limit:
                break

    return _compact({"total": len(results), "results": results})


# ---------------------------------------------------------------------------
# Tool 3: haver_browse
# ---------------------------------------------------------------------------

@mcp.tool()
def haver_browse(database: str, cursor: str = "", limit: int = 25) -> str:
    """Browse series in a Haver database alphabetically.

    Works for all 138 databases. Returns paginated results with a 'next'
    cursor for continuation.

    Args:
        database: Database code (e.g. 'USECON', 'GERMANY', 'WBPRICES')
        cursor: Start from this series code alphabetically (for pagination)
        limit: Max results (default 25, max 100)
    """
    limit = min(max(1, limit), 100)
    db_upper = database.upper().strip()

    if _is_local_db(db_upper):
        return _browse_local(db_upper, cursor.upper().strip(), limit)
    return _browse_api(db_upper, cursor.upper().strip(), limit)


def _browse_local(database: str, cursor: str, limit: int) -> str:
    """Browse local SQLite metadata."""
    conn = sqlite3.connect(str(METADATA_DB))
    conn.row_factory = sqlite3.Row

    if cursor:
        sql = """
            SELECT series_code, description, frequency, start_date, end_date
            FROM series
            WHERE database = ? AND series_code > ?
            ORDER BY series_code
            LIMIT ?
        """
        rows = conn.execute(sql, [database, cursor, limit]).fetchall()
    else:
        sql = """
            SELECT series_code, description, frequency, start_date, end_date
            FROM series
            WHERE database = ?
            ORDER BY series_code
            LIMIT ?
        """
        rows = conn.execute(sql, [database, limit]).fetchall()
    conn.close()

    results = []
    for r in rows:
        results.append({
            "code": f"{r['series_code']}@{database}",
            "desc": _truncate(r["description"]),
            "freq": _freq_abbrev(r["frequency"]),
        })

    next_cursor = results[-1]["code"].split("@")[0] if results else ""
    return _compact({
        "db": database,
        "count": len(results),
        "next": next_cursor,
        "results": results,
    })


def _browse_api(database: str, cursor: str, limit: int) -> str:
    """Browse via Haver API. Uses a single page request (max 1000 series)."""
    try:
        # Call API directly — the client's get_series() without `like` fetches
        # ALL series (very slow for large DBs). We always use `page` param.
        url = f"{_HAVER_URL}/v4/database/{database}/series"
        params = {"per_page": min(limit, 1000)}
        if cursor:
            params["page"] = cursor
        r = requests.get(url, params=params, headers=_api_headers(), timeout=30)
        r.raise_for_status()
        series = r.json().get("data", [])
        items = series[:limit]
    except Exception as e:
        return _compact({"db": database, "count": 0, "next": "", "results": [], "error": str(e)})

    results = []
    for item in items:
        code = item.get("name", "")
        results.append({
            "code": f"{code}@{database}",
            "desc": _truncate(item.get("description", "")),
            "freq": _freq_abbrev(item.get("originalFrequency", "")),
        })

    next_cursor = results[-1]["code"].split("@")[0] if results else ""
    return _compact({
        "db": database,
        "count": len(results),
        "next": next_cursor,
        "results": results,
    })


# ---------------------------------------------------------------------------
# Tool 4: haver_list_databases
# ---------------------------------------------------------------------------

@mcp.tool()
def haver_list_databases(filter: str = "") -> str:
    """List available Haver databases with descriptions.

    Returns all 138 databases. Use the filter parameter to narrow results
    by keyword (searches database code and description).

    Args:
        filter: Optional keyword to filter databases (e.g. 'commodity', 'germany')
    """
    try:
        haver = _get_client()
        databases = haver.get_databases()
    except Exception as e:
        return _compact({"count": 0, "databases": [], "error": str(e)})

    # Get local series counts
    local_counts: dict = {}
    if _has_local_db():
        conn = sqlite3.connect(str(METADATA_DB))
        for row in conn.execute("SELECT code, series_count FROM databases"):
            local_counts[row[0]] = row[1]
        conn.close()

    filter_lower = filter.lower().strip()
    result = []
    for code, desc in databases.items():
        desc_str = desc or ""
        if filter_lower:
            if filter_lower not in code.lower() and filter_lower not in desc_str.lower():
                continue
        entry: dict = {"code": code, "name": _truncate(desc_str, 50)}
        if code in local_counts:
            entry["n"] = local_counts[code]
        result.append(entry)

    return _compact({"count": len(result), "databases": result})


# ---------------------------------------------------------------------------
# Tool 5: haver_series_info
# ---------------------------------------------------------------------------

@mcp.tool()
def haver_series_info(codes: list[str]) -> str:
    """Get detailed metadata for specific Haver series.

    Returns full (untruncated) descriptions, frequency, date range, source,
    and aggregation method. Useful after search to inspect series before
    fetching data.

    Args:
        codes: List of series codes in 'SERIES@DATABASE' format (max 10).
               Example: ['GDP@USECON', 'BANAX@ENERGY']
    """
    codes = codes[:10]
    results = []

    for full_code in codes:
        parts = full_code.strip().split("@")
        if len(parts) != 2:
            results.append({"code": full_code, "error": "Invalid format, use SERIES@DATABASE"})
            continue
        series_code, database = parts[0].upper(), parts[1].upper()

        # Try local first
        if _is_local_db(database) and _has_local_db():
            conn = sqlite3.connect(str(METADATA_DB))
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM series WHERE series_code = ? AND database = ?",
                [series_code, database],
            ).fetchone()
            conn.close()
            if row:
                results.append({
                    "code": f"{series_code}@{database}",
                    "desc": (row["description"] or "").strip(),
                    "freq": row["frequency"] or "",
                    "range": f"{row['start_date'] or '?'} - {row['end_date'] or '?'}",
                    "source": (row["source"] or "").strip(),
                    "agg": (row["aggregation"] or "").strip(),
                })
                continue

        # Fall back to API
        try:
            haver = _get_client()
            meta = haver.read(database=database, series=series_code)
            results.append({
                "code": f"{series_code}@{database}",
                "desc": (meta.get("description") or "").strip(),
                "freq": _freq_abbrev(meta.get("originalFrequency", "")),
                "source": (meta.get("sourceName") or "").strip(),
                "points": meta.get("dataPointCount", ""),
            })
        except Exception as e:
            results.append({"code": f"{series_code}@{database}", "error": str(e)})

    return _compact({"series": results})


# ---------------------------------------------------------------------------
# Tool 6: haver_get_data
# ---------------------------------------------------------------------------

@mcp.tool()
def haver_get_data(haver_codes: list[str], start_date: str = "", end_date: str = "") -> str:
    """Fetch time series data for one or more Haver series.

    Returns data grouped by series with compact [date, value] arrays.

    Args:
        haver_codes: List of series codes in 'SERIES@DATABASE' format
                     (e.g. ['GDP@USECON', 'BANAX@ENERGY'])
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)
    """
    haver = _get_client()
    series_results = []

    # Fetch each series individually to handle errors gracefully
    for code in haver_codes:
        try:
            df = haver.read_df(haver_codes=[code])
        except (KeyError, Exception):
            # Fallback: use read() API directly and build DataFrame
            try:
                parts = code.strip().split("@")
                if len(parts) != 2:
                    series_results.append({"code": code, "error": "Invalid format"})
                    continue
                import pandas as pd
                raw = haver.read(database=parts[1], series=parts[0])
                data_points = raw.get("dataPoints", [])
                if not data_points:
                    series_results.append({"code": code, "n": 0, "data": []})
                    continue
                points = [[p["date"], p.get("nSeriesData")] for p in data_points]
                if start_date:
                    points = [p for p in points if p[0] >= start_date]
                if end_date:
                    points = [p for p in points if p[0] <= end_date]
                truncated = len(points) > 2000
                if truncated:
                    points = points[-2000:]
                series_results.append({
                    "code": code.lower(),
                    "n": len(points),
                    "data": points,
                    **({"truncated": True} if truncated else {}),
                })
                continue
            except Exception as e2:
                series_results.append({"code": code, "error": str(e2)})
                continue

        # Apply date filters
        if "date" in df.columns:
            df["date"] = df["date"].astype(str)
            if start_date:
                df = df[df["date"] >= start_date]
            if end_date:
                df = df[df["date"] <= end_date]

        if "variable" in df.columns:
            for var_name, group in df.groupby("variable"):
                points = group[["date", "value"]].values.tolist()
                truncated = len(points) > 2000
                if truncated:
                    points = points[-2000:]
                series_results.append({
                    "code": str(var_name),
                    "n": len(points),
                    "data": points,
                    **({"truncated": True} if truncated else {}),
                })
        else:
            points = df[["date", "value"]].values.tolist() if "value" in df.columns else []
            truncated = len(points) > 2000
            if truncated:
                points = points[-2000:]
            series_results.append({
                "code": code.lower(),
                "n": len(points),
                "data": points,
                **({"truncated": True} if truncated else {}),
            })

    return _compact({"series": series_results})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
