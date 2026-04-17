"""Haver Analytics MCP Server.

Token-efficient tools for discovering and fetching data from Haver Analytics
across all 138 databases (US + international) via the Haver REST API.
"""

import fnmatch
import json
import os

import requests
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("haver")

_HAVER_URL = "https://api.haverview.com"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _truncate(text: str, max_len: int = 80) -> str:
    """Truncate text to max_len chars, appending '...' if needed."""
    if not text or len(text) <= max_len:
        return (text or "").strip()
    return text[: max_len - 3].strip() + "..."


def _compact(obj) -> str:
    """Compact JSON serialisation — no whitespace."""
    return json.dumps(obj, separators=(",", ":"), default=str)


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

    Returns compact results with truncated descriptions.

    Args:
        query: Search terms (e.g. 'TTF natural gas', 'GDP contribution')
        database: Optional database filter (e.g. 'USECON', 'GERMANY')
        limit: Max results to return (default 25, max 100)
    """
    limit = min(max(1, limit), 100)
    db_upper = database.upper().strip() if database else ""

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
        if db_upper and db_name.upper() != db_upper:
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
def haver_search_by_code(pattern: str, database: str, limit: int = 25) -> str:
    """Find Haver series by code pattern within a specific database.

    Use SQL-style wildcards: % matches any string, _ matches one character.
    The pattern must start with a non-wildcard prefix. Best for navigating
    Haver's code conventions (e.g. R134G% for all Germany yield series in
    INTDAILY, PAT% for GDP contributions in USECON).

    Args:
        pattern: Code pattern with wildcards (e.g. 'R134G%', 'PAT%').
                 Must start with a non-wildcard prefix.
        database: Database to search (required, e.g. 'USECON', 'INTDAILY')
        limit: Max results (default 25, max 100)
    """
    limit = min(max(1, limit), 100)
    db_upper = database.upper().strip()
    pattern_upper = pattern.upper().strip()

    if not db_upper:
        return _compact({"total": 0, "results": [], "error": "database is required"})

    # Extract the prefix before the first wildcard
    prefix = pattern_upper.split("%")[0].split("_")[0]
    if not prefix:
        return _compact({"total": 0, "results": [], "error": "Pattern must start with a non-wildcard prefix"})

    # The API's `page` param is exclusive — it returns series AFTER the cursor.
    # Step back by one character so the page includes the prefix itself.
    cursor = prefix[:-1] + chr(ord(prefix[-1]) - 1)

    try:
        url = f"{_HAVER_URL}/v4/database/{db_upper}/series"
        params = {"per_page": 1000, "page": cursor}
        r = requests.get(url, params=params, headers=_api_headers(), timeout=30)
        r.raise_for_status()
        series = r.json().get("data", [])
    except Exception as e:
        return _compact({"total": 0, "results": [], "error": str(e)})

    glob_pattern = pattern_upper.replace("%", "*").replace("_", "?")

    results = []
    for item in series:
        code = item.get("name", "")
        if fnmatch.fnmatch(code.upper(), glob_pattern):
            results.append({
                "code": f"{code}@{db_upper}",
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

    Returns paginated results with a 'next' cursor for continuation.

    Args:
        database: Database code (e.g. 'USECON', 'GERMANY', 'WBPRICES')
        cursor: Start from this series code alphabetically (for pagination)
        limit: Max results (default 25, max 100)
    """
    limit = min(max(1, limit), 100)
    db_upper = database.upper().strip()
    cursor_upper = cursor.upper().strip()

    try:
        url = f"{_HAVER_URL}/v4/database/{db_upper}/series"
        params = {"per_page": min(limit, 1000)}
        if cursor_upper:
            params["page"] = cursor_upper
        r = requests.get(url, params=params, headers=_api_headers(), timeout=30)
        r.raise_for_status()
        series = r.json().get("data", [])
        items = series[:limit]
    except Exception as e:
        return _compact({
            "db": db_upper, "count": 0, "next": "", "results": [], "error": str(e),
        })

    results = []
    for item in items:
        code = item.get("name", "")
        results.append({
            "code": f"{code}@{db_upper}",
            "desc": _truncate(item.get("description", "")),
            "freq": _freq_abbrev(item.get("originalFrequency", "")),
        })

    next_cursor = results[-1]["code"].split("@")[0] if results else ""
    return _compact({
        "db": db_upper,
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

    filter_lower = filter.lower().strip()
    result = []
    for code, desc in databases.items():
        desc_str = desc or ""
        if filter_lower:
            if filter_lower not in code.lower() and filter_lower not in desc_str.lower():
                continue
        result.append({"code": code, "name": _truncate(desc_str, 50)})

    return _compact({"count": len(result), "databases": result})


# ---------------------------------------------------------------------------
# Tool 5: haver_series_info
# ---------------------------------------------------------------------------

@mcp.tool()
def haver_series_info(codes: list[str]) -> str:
    """Get detailed metadata for specific Haver series.

    Returns full (untruncated) descriptions, frequency, and source.
    Useful after search to inspect series before fetching data.

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
