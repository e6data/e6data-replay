"""Databricks SQL warehouse. Needs: pip install databricks-sql-connector

Settings come from options or environment variables, and are asked for if missing:
    --dbr-host       DATABRICKS_HOST       workspace host, e.g. dbc-xxxx.cloud.databricks.com
    --dbr-http-path  DATABRICKS_HTTP_PATH  SQL warehouse HTTP path, /sql/1.0/warehouses/<id>
    --dbr-catalog    DATABRICKS_CATALOG    catalog holding the tables
    --dbr-schema     DATABRICKS_SCHEMA     schema that plain table names resolve in
The token comes from DATABRICKS_TOKEN, or it is asked for (input hidden). It is never printed.
The result cache is turned off on every connection. Server timings are read after the run
from the Query History API.
"""
import getpass
import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from .base import Engine, close_quietly, error_text, with_deadline

LOGIN_TIMEOUT_S = 600  # a stopped SQL warehouse can take minutes to start


class Databricks(Engine):
    name = "databricks"
    server_fields = ("server_total_ms", "server_execution_ms", "server_compile_ms", "server_rows_read", "cached")
    connect_note = "a stopped SQL warehouse is started first, which can take a few minutes"

    @classmethod
    def add_arguments(cls, g):
        g.add_argument("--dbr-host", help="workspace host (default: DATABRICKS_HOST)")
        g.add_argument("--dbr-http-path", help="SQL warehouse HTTP path, /sql/1.0/warehouses/<id> "
                                               "(default: DATABRICKS_HTTP_PATH)")
        g.add_argument("--dbr-catalog", help="catalog holding the tables (default: DATABRICKS_CATALOG)")
        g.add_argument("--dbr-schema", help="schema that plain table names resolve in (default: DATABRICKS_SCHEMA)")

    def __init__(self, args):
        super().__init__(args)

        def ask(label, value, env):
            value = (value or os.environ.get(env, "")).strip()
            if value:
                return value
            if not sys.stdin.isatty():
                sys.exit(f"Databricks {label} is missing: set {env}")
            value = input(f"Databricks {label}: ").strip()
            if not value:
                sys.exit(f"Databricks {label} is needed for --engine databricks")
            return value
        self.host = re.sub(r"^https?://", "", ask("host (e.g. dbc-1234.cloud.databricks.com)",
                                                  args.dbr_host, "DATABRICKS_HOST")).rstrip("/")
        self.http_path = ask("HTTP path (/sql/1.0/warehouses/<id>)", args.dbr_http_path, "DATABRICKS_HTTP_PATH")
        self.catalog = ask("catalog", args.dbr_catalog, "DATABRICKS_CATALOG")
        self.schema = ask("schema", args.dbr_schema, "DATABRICKS_SCHEMA")
        token = os.environ.get("DATABRICKS_TOKEN", "").strip()
        if not token:
            try:
                token = getpass.getpass("Databricks personal access token (input hidden): ").strip()
            except (EOFError, KeyboardInterrupt):
                token = ""
        if not token:
            sys.exit("a Databricks token is needed: set DATABRICKS_TOKEN or type it when asked")
        self.token = token

    @property
    def warehouse(self):
        return self.http_path.rsplit("/", 1)[-1]

    def describe(self):
        return f"warehouse {self.warehouse}, dataset {self.catalog}.{self.schema}, result cache off"

    def connect(self):
        try:
            from databricks import sql as dbsql
        except ImportError:
            return None, "the Databricks connector is not installed: pip install databricks-sql-connector"
        # The connector logs its own retries and telemetry; errors still reach the results file.
        logging.getLogger("databricks").setLevel(logging.CRITICAL)
        logging.getLogger("urllib3").setLevel(logging.CRITICAL)

        def go():
            c = dbsql.connect(server_hostname=self.host, http_path=self.http_path, access_token=self.token,
                              catalog=self.catalog, schema=self.schema)
            try:  # a cached answer would not be a real run
                cur = c.cursor()
                try:
                    cur.execute("SET use_cached_result = false")
                finally:
                    cur.close()
            except Exception as exc:
                close_quietly(c)
                raise RuntimeError(f"could not turn off the Databricks result cache: {error_text(exc)}")
            return c
        conn, exc, timed_out = with_deadline(go, LOGIN_TIMEOUT_S)
        if timed_out:
            return None, (f"no answer from Databricks {self.host} within {LOGIN_TIMEOUT_S}s: check the host, "
                          "the HTTP path and that the SQL warehouse can start")
        if exc is not None:
            msg = error_text(exc)
            return None, msg if msg.startswith("could not turn off") else f"Databricks login failed: {msg}"
        return conn, None

    def cancel(self, cursor):
        cursor.cancel()

    def history(self, qids, attempts=8, wait_s=3.0):
        """Server timings per statement id from the Query History API. The dotted filter_by
        form is required (the JSON form is a 400); FINISHED is recorded before the metrics
        are, so unfilled entries are asked again. Returns ({qid: timings}, warning)."""
        def one(qid):
            params = urllib.parse.urlencode({"filter_by.statement_ids": qid, "include_metrics": "true"})
            req = urllib.request.Request(f"https://{self.host}/api/2.0/sql/history/queries?{params}",
                                         headers={"Authorization": f"Bearer {self.token}"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                payload = json.load(resp)
            for q in payload.get("res") or []:
                if q.get("query_id") != qid or q.get("status") != "FINISHED":
                    continue
                m = q.get("metrics") or {}
                if m.get("execution_time_ms") is None:
                    return None
                return {"server_compile_ms": m.get("compilation_time_ms"),
                        "server_execution_ms": m.get("execution_time_ms"),
                        "server_total_ms": q.get("duration"), "server_rows_read": m.get("rows_read_count"),
                        "cached": m.get("result_from_cache")}
            return None

        def safe(qid):
            try:
                return qid, one(qid), None
            except urllib.error.HTTPError as exc:
                return qid, None, f"HTTP {exc.code}"
            except Exception as exc:
                return qid, None, " ".join(str(exc).split())[:150]

        found, pending, last_err = {}, list(dict.fromkeys(qids)), ""
        for attempt in range(attempts):
            with ThreadPoolExecutor(4) as ex:  # few at a time: the API rate-limits bursts (HTTP 429)
                got = list(ex.map(safe, pending))
            if any(err in ("HTTP 401", "HTTP 403") for _, _, err in got):
                return found, "the Databricks token cannot read query history, so Databricks server timings are blank"
            found.update({qid: t for qid, t, _ in got if t})
            pending = [qid for qid, t, _ in got if not t]
            errs = [err for _, t, err in got if err]
            last_err = errs[-1] if errs else last_err
            if not pending:
                break
            if attempt < attempts - 1:
                time.sleep(wait_s * (attempt + 1))  # wait longer each round
        if pending:
            return found, (f"Databricks server timings not found for {len(pending)} execution(s)"
                           + (f" (last error: {last_err})" if last_err else " (not in query history yet)"))
        return found, None

    def finish(self, results, t0, t1):
        facts = [("databricks_dataset", f"{self.catalog}.{self.schema}"), ("databricks_warehouse", self.warehouse)]
        notes = []
        qids = [r["query_id"] for r in results if r["status"] == "OK" and r["query_id"]]
        if qids and not self.args.no_server_stats:
            print(f"\nfetching Databricks server timings for {len(qids)} execution(s) from query history ...",
                  flush=True)
            found, warning = self.history(qids)
            for r in results:
                if r["query_id"] in found:
                    r.update(found[r["query_id"]])
            if warning:
                notes.append(warning)
        return facts, notes, []
