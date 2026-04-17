# haver-mcp-server

A [Model Context Protocol](https://modelcontextprotocol.io) server that gives
Claude (or any MCP-compatible LLM client) structured, token-efficient access
to [Haver Analytics](https://www.haver.com/) — covering all 138 of Haver's US
and international databases.

It exposes six tools for series discovery and data retrieval:

| Tool | Purpose |
|------|---------|
| `haver_search` | Keyword search across databases (e.g. `"TTF natural gas"`) |
| `haver_search_by_code` | Pattern search with SQL wildcards (e.g. `"R134G%"` for German yields) |
| `haver_browse` | Paginate through a database alphabetically |
| `haver_list_databases` | List the 138 databases, optionally filtered by keyword |
| `haver_series_info` | Fetch full metadata for specific series before pulling data |
| `haver_get_data` | Fetch time series data with optional date filters |

## Prerequisites

- An active **Haver Analytics** subscription and API key. See your Haver
  account rep for access.
- [`uv`](https://docs.astral.sh/uv/) installed on your machine. One-liner:
  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```

You do **not** need to clone this repo or manage a virtual environment —
`uvx` handles everything from the git URL.

## Install

### Claude Code

Add this entry to your `.mcp.json` (project-level, or workspace-level):

```json
{
  "mcpServers": {
    "haver": {
      "command": "uvx",
      "args": [
        "--from",
        "git+https://github.com/joshua-bailey/haver-mcp-server.git",
        "haver-mcp-server"
      ],
      "env": {
        "HAVER_API_KEY": "your-haver-api-key-here"
      }
    }
  }
}
```

Then restart Claude Code. The six `haver_*` tools should appear.

### Claude Desktop

Same JSON snippet, placed under `mcpServers` in:

- macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
- Windows: `%APPDATA%\Claude\claude_desktop_config.json`

### Pinning a version

`uvx` installs the latest commit on `main` by default. To pin to a tagged
release, append `@<tag>` to the git URL, e.g.:

```
"git+https://github.com/joshua-bailey/haver-mcp-server.git@v0.4.0"
```

## Environment variables

| Variable | Required? | Purpose |
|----------|-----------|---------|
| `HAVER_API_KEY` | **Yes** | Your Haver Analytics API key. |

## Using it

Once the MCP is registered, ask Claude things like:

- "Search Haver for TTF natural gas series"
- "Show me all the German yield series in INTDAILY — codes starting with `R134G`"
- "Pull GDP@USECON and PCE@USECON since 2020"
- "List Haver databases with 'commodity' in the name"

Claude will pick the right tool and call it.

## Credits and upstream dependency

This server is a thin MCP wrapper around
[**`haver-api`**](https://github.com/LucaMingarelli/haver) by
**Luca Mingarelli**, which handles authentication and data retrieval against
the Haver Analytics REST API. Please acknowledge that package separately
when using this server:

- Source: https://github.com/LucaMingarelli/haver
- PyPI: https://pypi.org/project/haver-api/
- License: **CC BY-NC-SA 4.0** (Attribution-NonCommercial-ShareAlike 4.0
  International). Review the upstream license before any commercial use —
  `haver-api`'s terms apply independently of this wrapper's MIT license.

`haver-api` is installed from PyPI as a normal dependency when you run
`uvx`; we do not redistribute its source code here.

## License

MIT — see [LICENSE](LICENSE). Applies to the wrapper code in this
repository only. Dependencies retain their own licenses.
