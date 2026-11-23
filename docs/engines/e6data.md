# Running on e6data

`--engine e6data` runs the queries on an e6data cluster. You log in as yourself, with your own personal access token, so your runs also show in the e6data console's **Query History**.

## Setup

1. **Install the connector** (one time, in your virtual environment):

   ```
   pip install e6data-python-connector==3.0.0
   ```

2. **Make sure the cluster is running.** In the e6data console, open **Workspaces** and check that your workspace shows `Running` (if it is `Disabled`, click the play button in its Actions column). Click **Open**, then **Clusters**, and check that your cluster shows `Running`. You can also let the script do this step: it resumes a suspended cluster when it logs in, and waits until the cluster is ready before it starts timing. If the workspace has a warm pool, resume it too (**Warm Pools**, play button): it keeps machines ready so the cluster resumes in about a minute.

3. **Create a personal access token** (one time):
   1. Sign in to the e6data console and open your workspace.
   2. Open the avatar menu (top right), then **User settings**.
   3. Open the **Access tokens** tab and select **Create**.
   4. Enter a comment (3 to 200 characters), for example `replay script`.
   5. Set **Token Expiry** in days (default 90). Don't tick "Never expires".
   6. Select **Create** and copy the token straight away (it starts with `e6pat_`). It is shown only once. If you lose it, create a new one and delete the old one.

4. **Set the connection details** in the same terminal, before you run the script. Use your e6data login email and your token. The host is your workspace's host name. The catalog and schema are where the tables are, so plain table names in the CSV resolve there.

   macOS / Linux:

   ```
   export E6_USER="you@example.com"
   export E6_TOKEN="e6pat_..."
   export E6_HOST="<id>.e6.run"
   export E6_CLUSTER="<cluster>"
   export E6_CATALOG="<catalog>"
   export E6_SCHEMA="<schema>"
   ```

   Windows (PowerShell): `$env:E6_USER = "you@example.com"`, and so on.

5. **Run:**

   ```
   python -m replay examples/queries.csv --engine e6data --runs 3
   ```

The `e6data:` line printed before the run ends with `as <user>`, which tells you who it's running as. The script stops if any setting is missing.

## Options

| Option | Meaning | Default |
|---|---|---|
| `--e6-host HOST` | Workspace host | `E6_HOST` |
| `--e6-port N` | Port | 443 |
| `--e6-cluster NAME` | Cluster to run on | `E6_CLUSTER` |
| `--e6-catalog NAME` | Catalog holding the tables | `E6_CATALOG` |
| `--e6-schema NAME` | Schema that plain table names resolve in | `E6_SCHEMA` |
| `--e6-transport grpc\|http` | How to talk to e6data. `http` uses the REST API over HTTP/1.1. See [Behind a proxy that blocks gRPC](#behind-a-proxy-that-blocks-grpc-for-example-zscaler) | `E6_TRANSPORT`, else `grpc` |
| `--e6-no-usage` | Skip reading core-hours and cost after the run | usage on |

`E6_USER` and `E6_TOKEN` are read only from the environment, never from options.

SQL reference: [e6data SQL functions](https://docs.e6data.com/query/sql/functions/index).

## What it measures

Server timings come from one extra call after each query (not counted in `client_seconds`). It fills these columns:

| Column | e6data meaning |
|---|---|
| `server_total_ms` | e6data's end-to-end time for the query, measured on the server |
| `server_execution_ms` | Execution time |
| `server_queue_ms`, `server_planning_ms` | Queueing and planning time |
| `cached` | Whether the answer came from e6data's result cache |
| `query_id` | e6data query id. Search for it in the console's **Query History** |

The extra call lowers queries per second. For throughput runs, add `--no-server-stats`.

## Warm-up

Before timing begins, the script runs the `--warmup-sql` query once, untimed. Use a small scan of one of your tables, for example `--warmup-sql "select count(*) from date_dim"`, so the workers are awake. Then it waits until the cluster is at its usual size (up to 5 minutes). If the login took long because the cluster had been suspended, it first waits 1 minute.

## Usage and cost

At the end of every run, the script reads e6data's usage view, `system.usage.core_usage`. It prints the core-hours and cost of the cluster for the time the run was going. Each usage interval counts only for the part inside the run. The summary file has the same figures as `e6data_core_hours` and `e6data_cost_usd`.

This is the whole cluster's usage. If others query the same cluster at the same time, their share is included. New usage can take a few minutes to appear after the start of each hour. The script waits up to about 4 minutes for it. If usage is still missing, it prints a warning that the figure is low. Rerun the SQL below a few minutes later for the full number:

```
SELECT * FROM system.usage.core_usage WHERE cluster_name = '<cluster>' ORDER BY active_start DESC
```

When you finish for the day, suspend the cluster (and the warm pool, if you use one) in the console with the pause button. A cluster also suspends by itself after a period without queries.

## Behind a proxy that blocks gRPC (for example Zscaler)

The connector talks gRPC, which needs HTTP/2. Some proxies that inspect HTTPS, such as Zscaler, only pass HTTP/1.1, and the login fails with `missing selected ALPN property`. Use the HTTP transport instead. It sends the same queries to the same cluster through e6data's REST API over HTTP/1.1, and needs no connector:

```
export E6_TRANSPORT=http
python -m replay tpcds_103_queries.csv --engine e6data --runs 3
```

(or add `--e6-transport http` to the command). The output has the same columns: server timings, query ids, usage and cost are read from the same e6data data as over gRPC.

- If the proxy re-signs certificates and Python reports `CERTIFICATE_VERIFY_FAILED`, set `E6_CA_BUNDLE` to the proxy's root certificate (a `.pem` file).
- Behind an explicit proxy, set `https_proxy` as usual.
- `client_seconds` includes the HTTP requests for each query and the proxy's inspection time. A query that runs past 10 s is polled, which can add up to about 0.25 s. Compare `client_seconds` only between runs that used the same transport; the summary file records it as `e6data_transport`. Server timings measure the same thing on both. With `--pattern` at a low rate, a connection left idle for over 20 s is reopened before its next query, so that query's `client_seconds` includes a new TLS handshake.

## Troubleshooting

| Message | What to do |
|---|---|
| `could not reach ... within 420s` | Outbound HTTPS (port 443) is blocked, or a proxy is needed. Set `https_proxy` or try another network |
| `missing selected ALPN property` | A proxy is blocking gRPC. Use `--e6-transport http` (see above) |
| `e6data login failed` / `PERMISSION_DENIED` | Check that `E6_USER` is your login email (not a display name), that the token hasn't expired, and that your user has a role on the cluster and the catalog |
| `the cluster is suspended and could not be resumed from here` | The script normally resumes the cluster at login (a few minutes). If that fails, resume it in the console, then rerun |
| `{INVALID QUERY} ... [line x, col y]` | SQL syntax error, or an unknown table or column. Check `E6_CATALOG` and `E6_SCHEMA` |
| Queries stop at a fixed time | The cluster's query timeout. Ask your e6data admin for its value |

For help, send the e6data team the `query_id` and the error.

## Your token

Your token has your role's permissions, and it stops working when your account is removed. Treat it like a password. Delete it under **User settings > Access tokens** when you no longer need it.
