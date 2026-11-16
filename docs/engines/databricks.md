# Running on Databricks

`--engine databricks` runs the queries on a Databricks SQL warehouse. Point it at your own workspace and warehouse, with the tables the CSV uses.

## Setup

1. **Install the connector** (one time, in your virtual environment):

   ```
   pip install databricks-sql-connector
   ```

2. **Set the connection details.** Find the host and HTTP path under **SQL Warehouses**, then your warehouse, then **Connection details**. The catalog and schema are where the tables are, so plain table names in the CSV resolve there.

   ```
   export DATABRICKS_HOST="dbc-xxxx.cloud.databricks.com"
   export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/<id>"
   export DATABRICKS_CATALOG="<catalog>"
   export DATABRICKS_SCHEMA="<schema>"
   ```

   Windows (PowerShell): `$env:DATABRICKS_HOST = "..."`, and so on. The script asks for any value that is not set when it starts.

3. **Token.** The script asks for your Databricks personal access token at the start (typing is hidden). To skip the prompt, set `DATABRICKS_TOKEN`. The script never prints the token or writes it to a file.

4. **Run:**

   ```
   python -m replay examples/queries.csv --engine databricks
   ```

## Options

| Option | Meaning | Default |
|---|---|---|
| `--dbr-host HOST` | Workspace host | `DATABRICKS_HOST` |
| `--dbr-http-path PATH` | SQL warehouse HTTP path, `/sql/1.0/warehouses/<id>` | `DATABRICKS_HTTP_PATH` |
| `--dbr-catalog NAME` | Catalog holding the tables | `DATABRICKS_CATALOG` |
| `--dbr-schema NAME` | Schema that plain table names resolve in | `DATABRICKS_SCHEMA` |

## How it runs

- Each connection turns the result cache off (`SET use_cached_result = false`), so every query really runs. If that is refused, the script stops before running anything.
- Logging in starts a stopped warehouse. The script waits up to 10 minutes for it.
- Ctrl+C cancels the running queries.

## What it measures

After the run, the script reads Databricks' own timings from its Query History API (`/api/2.0/sql/history/queries`). This adds no calls during the run. It fills these columns:

| Column | Databricks meaning |
|---|---|
| `server_total_ms` | Databricks' own duration for the query, measured on the server |
| `server_execution_ms`, `server_compile_ms` | Execution and compilation time |
| `server_rows_read` | Rows Databricks read |
| `cached` | Whether the answer came from its cache |
| `query_id` | Databricks statement id |

If the token can't read query history, those columns stay blank and a note says so. `--no-server-stats` skips the history lookup.

The summary file also records `databricks_dataset` and `databricks_warehouse`.

## Troubleshooting

| Message | What to do |
|---|---|
| `no answer from Databricks ... within 600s` | Check the host, the HTTP path, and that the SQL warehouse can start |
| `Databricks login failed` | Check the token and that it can use the warehouse |
| `could not turn off the Databricks result cache` | The warehouse refused `SET use_cached_result = false`. Check your permissions on the warehouse |
| `the Databricks token cannot read query history` | Server timings stay blank. Use a token that can read query history, or rely on `client_seconds` |
