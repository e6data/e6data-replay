# e6data-replay

`replay` runs the SQL queries in a CSV file on a query engine. It can run them one at a time or on a fixed load pattern (`--pattern`), and it can run them on several engines at once. It writes every execution to a results CSV: the time measured by the script, and the engine's own server-side timings.

Each engine is one plug-in file. Two engines are included as examples:

| Engine | `--engine` | Setup and options |
|---|---|---|
| e6data | `e6data` | [docs/engines/e6data.md](docs/engines/e6data.md) |
| Databricks SQL warehouse | `databricks` | [docs/engines/databricks.md](docs/engines/databricks.md) |

To run on another engine, add a plug-in: [docs/adding-an-engine.md](docs/adding-an-engine.md).

## Quick start

1. **Install Python 3.9 or newer**, then create a virtual environment in this folder:

   ```
   python3 -m venv .venv
   source .venv/bin/activate
   ```

   Windows (PowerShell): `py -m venv .venv`, then `.venv\Scripts\Activate.ps1`. If PowerShell refuses to run `Activate.ps1`, first run `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`.

2. **Install the connector and set the connection details for your engine.** Follow its page: [e6data](docs/engines/e6data.md#setup) or [Databricks](docs/engines/databricks.md#setup).

3. **Run** the three example queries first, to check the connection:

   ```
   python -m replay examples/queries.csv --engine e6data
   ```

4. **Run the full TPC-DS set**, or your own CSV ([Prepare the queries CSV](#prepare-the-queries-csv)):

   ```
   python -m replay tpcds_103_queries.csv --engine e6data --runs 3
   ```

`tpcds_103_queries.csv` holds the 99 standard TPC-DS queries. Queries 14, 23, 24 and 39 each have an a and b version, which makes 103. It has one SQL column per engine (`QUERY_ENGINE_1`, `QUERY_ENGINE_2`). Today both hold the same SQL. The script uses the first. To use the second, add `--sql-column QUERY_ENGINE_2`.

Run from the folder that contains the `replay` folder. The machine needs outbound HTTPS (port 443) to the engine. Behind a corporate proxy, set the `https_proxy` environment variable.

## Prepare the queries CSV

Any CSV with one column of SQL queries works. The column names don't matter. The script finds the SQL column by its content (the cells that start with SELECT, WITH and so on). It takes query names from a column whose header contains name, id, label, key or title. Without one, queries are named q001, q002, and so on. It prints which columns it used. `examples/queries.csv` is a small example (TPC-DS tables) to start from.

```
name,sql
store_sales_count,select count(*) from store_sales
sales_by_year,"select d_year, count(*) from store_sales join date_dim on ss_sold_date_sk = d_date_sk group by d_year"
```

- Wrap a query in double quotes if it contains commas or line breaks. Excel and Google Sheets do this automatically when you save as CSV.
- Comma, semicolon and tab separated files all work. UTF-8 is preferred.
- If more than one column holds SQL (for example one per engine), the script uses the first and lists the others. Pick another with `--sql-column` (a header or a column number, counting from 1), and the name column with `--name-column`.
- Use plain table names (`store_sales`). The engine resolves them in the catalog and schema you set for it.
- Put one statement per row. A trailing `;` is fine. The script skips rows that are empty or hold only `--` comments.
- A file with only a `sql` column (or no header, one query per line) is fine too. Commas inside the SQL are kept.
- If two rows share a name, the second is reported as `name#2`.

The same CSV runs on every engine, so use SQL that all the engines you pick accept.

## Run

```
python -m replay examples/queries.csv --engine e6data
```

| Option | Meaning | Default |
|---|---|---|
| `--engine NAME` | Engine to run on. Give several, separated by commas (`e6data,databricks`), to run the same queries on each on the same schedule | required |
| `--runs N` | Run each query N times | 1 |
| `--pattern FILE` | Start queries in steps of different rates: a JSON list of `{"Seconds": S, "QPS": N}` steps, run in order, cycling through the CSV (see below) | off |
| `--max-inflight N` | With `--pattern`: the most queries this machine keeps open at once, per engine | 64 |
| `--warmup-sql SQL` | Untimed query run once on each engine before timing begins. A small table scan wakes the workers; `''` skips it | `SELECT 1` |
| `--no-server-stats` | Skip reading the engines' server-side timings | timings on |
| `--out FILE` | Results file | `results_<date>_<time>.csv` |
| `--sql-column NAME` | Column holding the SQL (header or number, from 1) | found automatically |
| `--name-column NAME` | Column holding query names | found automatically |

Each engine adds its own options (host, catalog and so on). They are listed on its page and in `python -m replay --help`.

Example, each query 3 times:

```
python -m replay examples/queries.csv --engine e6data --runs 3 --out results_3runs.csv
```

### Several engines at once

`--engine e6data,databricks` sends every query to both engines. Both get the same queries in the same order, starting at the same moment. With `--pattern`, every query also starts at the same time on each engine (within a few ms). Each engine has its own connections, so a slow engine never holds up another. With `--runs`, each engine works through the list at its own pace.

### Load pattern

`examples/qps_pattern.json` holds these steps. It starts 275 queries over 98 s:

```
[
  {"Seconds": 10, "QPS": 2},
  {"Seconds": 8, "QPS": 5},
  {"Seconds": 5, "QPS": 1},
  {"Seconds": 5, "QPS": 4},
  {"Seconds": 15, "QPS": 2},
  {"Seconds": 15, "QPS": 4},
  {"Seconds": 20, "QPS": 2},
  {"Seconds": 10, "QPS": 4},
  {"Seconds": 10, "QPS": 2}
]
```

```
python -m replay examples/queries.csv --engine e6data --pattern examples/qps_pattern.json
```

`--pattern` starts queries at fixed times, whether or not earlier ones have finished. If the engine falls behind, queries queue on the engine, as they would with real users. At the end the script prints the arrivals line: queries started, how late they started, the peak number running on the engine, and, when arrivals stopped, how many were running and how many were still waiting for a connection. The summary file records the same.

The script never has more than `--max-inflight` queries open on an engine at once. If start delays are above 1 s, the engine saw at most that many at once rather than the full rate. Add `--max-inflight 200` and rerun.

For throughput runs, add `--no-server-stats` if the engine reads its timings with an extra call after each query (the engine's page says so). That call lowers queries per second.

### Stopping a run

Press Ctrl+C to stop. The script cancels the running queries and waits up to 30 s. Queries that stop in time are written as `CANCELLED`. Queries still running after that are counted as "not run". Press Ctrl+C a second time to quit at once. Queries already sent then finish or time out on the engine. Everything completed so far stays in the results file.

## Results

At the end the script prints, for each engine:

- a table per query: successful runs, then median, min and max time measured by the script, and the engine's median server time;
- an overall line: queries per second (successful only), and latency p50, p95, max and mean across all successful executions. The percentiles use linear interpolation, the same as Excel PERCENTILE.INC.

The exit code is 0 if every execution succeeded, otherwise 1.

It writes two files:

- the results file (`--out`), with one row per execution (columns below);
- a summary file next to it, named `<results name>_summary.csv`, for the whole run, in milliseconds over successful executions:
  - one row per statistic: p50, p75, p90, p99, max, mean and total;
  - one column per engine and time: `<engine>_client_ms` (measured by the script) and `<engine>_server_total_ms` and the other server times the engine reports;
  - underneath, the SUCCESS / FAILED / CANCELLED / NOT_RUN counts per engine, the run mode, and anything the engine adds (dataset, cost).

The results CSV has one row per execution:

| Column | Meaning |
|---|---|
| `engine` | Engine the query ran on |
| `name`, `run` | Query name and run number |
| `status` | `OK`, `FAILED`, or `CANCELLED` (stopped with Ctrl+C) |
| `started` | When the query was sent (local time, ms) |
| `client_seconds` | Time measured by the script: sending the query, running it, and fetching all result rows over the network |
| `rows` | Rows returned |
| `server_total_ms` | The engine's own end-to-end time for the query, measured on the server |
| `server_execution_ms`, `server_queue_ms`, `server_planning_ms`, `server_compile_ms` | The engine's execution, queueing, planning and compilation time |
| `server_rows_read`, `cached` | Rows the engine read, and whether the answer came from its result cache |
| `query_id` | The engine's id for the query, to look it up in its query history |
| `error` | Error message for failed queries, cut to 1,000 characters |

The `server_*` and `cached` columns appear only if one of the chosen engines fills them. Each engine page lists which ones it fills.

### Timing notes

- `client_seconds` includes the network between your machine and the engine: each query costs a few round trips. From a machine far from the engine's cloud region, each query can take about 1 s longer than the server time. For server-side time, use `server_total_ms`, or run the script from a machine in the same region.
- Fetching very large results adds transfer time to `client_seconds`. Prefer aggregates or `LIMIT` for timing runs.
- The first run of a query after a period of inactivity is usually slower, because table metadata is loaded. Run with `--runs 2` or more, and report run 1 (cold) and runs 2 onward (warm) separately. The `run` column tells them apart. The summary file covers all runs together. For a warm-only summary, run once to warm up, then run again with a new `--out`.
- Turn off the engine's result cache, or check the `cached` column, so that every query really runs.

## Troubleshooting

| Message | What to do |
|---|---|
| `could not reach ...` / `no answer from ...` | Outbound HTTPS (port 443) is blocked, or a proxy is needed. Set `https_proxy` or try another network |
| `... is missing: set ...` | A connection setting for the engine is not set. See the engine's page |
| `... connector is not installed` | Install the connector named in the message, in the same virtual environment |
| `cannot write ...` | The results file is open in Excel or the folder is read-only. Close it or pass another `--out` |
| A query seems stuck | The network connection probably dropped (the script has no deadline of its own). Press Ctrl+C and rerun |

Engine-specific messages are on each engine's page.

## Security

Access tokens work like passwords. Keep them to yourself, and set them as environment variables each time (never in a file or script). Never commit them to a code repository. The script never prints a token or writes it to a file.

## Layout

```
tpcds_103_queries.csv   the 103 TPC-DS queries
replay/
  cli.py              options, scheduling, results and report
  queries.py          reads the queries CSV
  report.py           percentiles and the summary file
  engines/
    base.py           the interface every engine implements
    e6data.py         e6data
    databricks.py     Databricks SQL warehouse
examples/
  queries.csv         three TPC-DS queries
  qps_pattern.json    a load pattern
docs/
  engines/            one page per engine
  adding-an-engine.md
```
