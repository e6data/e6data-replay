"""e6data. Needs: pip install e6data-python-connector==3.0.0

Credentials come from E6_USER (login email) and E6_TOKEN (personal access token, e6pat_...).
The rest from options or environment variables:
    --e6-host     E6_HOST      workspace host, e.g. <id>.e6.run
    --e6-cluster  E6_CLUSTER   cluster to run on
    --e6-catalog  E6_CATALOG   catalog holding the tables
    --e6-schema   E6_SCHEMA    schema (database) that plain table names resolve in
"""
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

from .base import Engine, close_quietly, error_text, with_deadline

LOGIN_TIMEOUT_S = 420   # covers resuming a suspended cluster (the connector waits up to 5 min)
RESUMED_LOGIN_S = 10    # a login slower than this means the cluster was suspended and has just been resumed
USAGE_VIEW = "system.usage.core_usage"
SUSPENDED_HINT = ("the cluster is suspended and could not be resumed from here: resume it in the e6data "
                  "console, then rerun")
QUERY_ID_AT_START = re.compile(r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})")


def _utc_epoch(v):
    """A core_usage timestamp (datetime or ISO text) as epoch seconds; naive values are UTC."""
    if isinstance(v, (int, float)):
        return float(v)
    if not isinstance(v, datetime):
        v = datetime.fromisoformat(str(v).strip().replace("Z", "+00:00").replace(" ", "T", 1))
    if v.tzinfo is None:
        v = v.replace(tzinfo=timezone.utc)
    return v.timestamp()


class E6Data(Engine):
    name = "e6data"
    server_fields = ("server_total_ms", "server_execution_ms", "server_queue_ms", "server_planning_ms", "cached")
    connect_note = "if the cluster is suspended it is resumed first, which can take a few minutes"

    @classmethod
    def add_arguments(cls, g):
        g.add_argument("--e6-host", help="workspace host, e.g. <id>.e6.run (default: E6_HOST)")
        g.add_argument("--e6-port", type=int, default=443, help="port (default 443)")
        g.add_argument("--e6-cluster", help="cluster to run on (default: E6_CLUSTER)")
        g.add_argument("--e6-catalog", help="catalog holding the tables (default: E6_CATALOG)")
        g.add_argument("--e6-schema", help="schema that plain table names resolve in (default: E6_SCHEMA)")
        g.add_argument("--e6-no-usage", action="store_true",
                       help=f"skip reading core-hours and cost from {USAGE_VIEW} after the run")

    def __init__(self, args):
        super().__init__(args)

        def setting(value, env, what):
            value = (value or os.environ.get(env, "")).strip()
            if not value:
                flag = f" or pass --e6-{what}" if what in ("host", "cluster", "catalog", "schema") else ""
                sys.exit(f"e6data {what} is missing: set {env}{flag} (see docs/engines/e6data.md)")
            return value
        self.user = setting(None, "E6_USER", "login email")
        self.token = setting(None, "E6_TOKEN", "access token")
        self.host = re.sub(r"^https?://", "", setting(args.e6_host, "E6_HOST", "host")).rstrip("/")
        self.port = args.e6_port
        self.cluster = setting(args.e6_cluster, "E6_CLUSTER", "cluster")
        self.catalog = setting(args.e6_catalog, "E6_CATALOG", "catalog")
        self.schema = setting(args.e6_schema, "E6_SCHEMA", "schema")

    def describe(self):
        return f"cluster {self.cluster}, dataset {self.catalog}.{self.schema}, as {self.user}"

    def connect(self):
        try:
            from e6data_python_connector import Connection
        except ImportError:
            return None, "the e6data connector is not installed: pip install e6data-python-connector==3.0.0"
        c = Connection(host=self.host, port=self.port, username=self.user, password=self.token,
                       catalog=self.catalog, database=self.schema, cluster_name=self.cluster,
                       secure=True, auto_resume=True)
        _, exc, timed_out = with_deadline(lambda: c.get_session_id, LOGIN_TIMEOUT_S)
        if timed_out:
            close_quietly(c)
            return None, (f"could not reach {self.host}:{self.port} within {LOGIN_TIMEOUT_S}s. Check that outbound "
                          "HTTPS (port 443) is allowed from this machine; behind a proxy, set the https_proxy "
                          "environment variable.")
        if exc is not None:
            close_quietly(c)
            msg = error_text(exc)
            return None, f"e6data login failed: {msg}" + (f"\n{SUSPENDED_HINT}" if "suspended" in msg.lower() else "")
        return c, None

    def cancel(self, cursor):
        qid = getattr(cursor, "query_id", None)
        if qid:
            cursor.cancel(qid)

    def query_stats(self, cursor):
        """One extra call to the engine (not in client_seconds)."""
        try:
            ea = cursor.explain_analyse()
            plan = json.loads(ea.get("planner") or "{}")
            return {"cached": ea.get("is_cached"),
                    "server_planning_ms": ea.get("parsing_time"),
                    "server_queue_ms": ea.get("queuing_time"),
                    "server_execution_ms": plan.get("total_query_time"),
                    "server_total_ms": plan.get("totalClientQueryTime")}
        except Exception:
            return None

    def query_id(self, cursor, error):
        qid = super().query_id(cursor, error)
        if not qid:  # failed queries carry their id at the start of the error
            m = QUERY_ID_AT_START.match(error or "")
            qid = m.group(1) if m else ""
        return qid

    def error_hint(self, error):
        return SUSPENDED_HINT if "suspended" in (error or "").lower() else None

    # ---- before the clock starts and after the run: e6data's usage view

    def _query(self, conn, sql):
        cur = conn.cursor()
        try:
            cur.execute(sql)
            return cur.fetchall()
        finally:
            close_quietly(cur)

    def prepare(self, login_seconds, max_wait_s=300, poll_s=15):
        """Wait (up to max_wait_s) until the usage view shows the cluster at its usual size,
        so a cluster that is still starting does not slow the first timed queries."""
        c, err = self.connect()
        if err:
            return err
        name = self.cluster.replace("'", "''")
        since = datetime.fromtimestamp(time.time() - 7 * 86400, timezone.utc).strftime("%Y-%m-%d 00:00:00")
        try:
            if login_seconds > RESUMED_LOGIN_S:  # just resumed: let it finish starting before checking its size
                print("  the cluster was just resumed; waiting 60 s for it to finish starting ...", flush=True)
                time.sleep(60)
            weight = {}  # cores -> seconds at that size over the last week
            for n, sec in self._query(c, f"SELECT total_cores, active_seconds FROM {USAGE_VIEW} "
                                         f"WHERE cluster_name = '{name}' AND active_end >= TIMESTAMP '{since}'"):
                weight[float(n or 0)] = weight.get(float(n or 0), 0.0) + float(sec or 0)
            usual = max(weight, key=weight.get) if weight else None
            if not usual:
                return None
            deadline = time.time() + max_wait_s
            while True:
                last = self._query(c, f"SELECT total_cores, active_end FROM {USAGE_VIEW} "
                                      f"WHERE cluster_name = '{name}' ORDER BY active_end DESC LIMIT 1")
                fresh = bool(last) and _utc_epoch(last[0][1]) >= time.time() - 120
                cores = float(last[0][0] or 0) if last else 0.0
                if not fresh or cores >= usual or time.time() >= deadline:
                    break
                print(f"  cluster is at {cores:g} of its usual {usual:g} cores; waiting for it to finish starting ...",
                      flush=True)
                time.sleep(poll_s)
            if fresh and cores < usual:
                return (f"the cluster was still at {cores:g} of its usual {usual:g} cores after "
                        f"{max_wait_s // 60} min, so timings may be slower")
            return None
        except Exception as exc:
            return f"cluster size check skipped: {error_text(exc)}"
        finally:
            close_quietly(c)

    def usage(self, t0, t1, attempts=9, wait_s=30.0):
        """Core-hours and cost for this cluster between t0 and t1 (epoch seconds), each usage
        interval counted only for the part inside the run. Returns (dict, None) or (None, error)."""
        since = datetime.fromtimestamp(t0 - 86400, timezone.utc).strftime("%Y-%m-%d 00:00:00")
        name = self.cluster.replace("'", "''")
        sql = (f"SELECT active_start, active_end, total_cores, core_hours, total_cost FROM {USAGE_VIEW} "
               f"WHERE cluster_name = '{name}' AND active_end >= TIMESTAMP '{since}'")
        c, err = self.connect()
        if err:
            return None, err
        try:
            for attempt in range(attempts):
                rows = self._query(c, sql)
                spans = [(_utc_epoch(r[0]), _utc_epoch(r[1]), float(r[2] or 0), float(r[3] or 0), float(r[4] or 0))
                         for r in rows if r[0] is not None and r[1] is not None]
                latest = max((e for _, e, *_ in spans), default=0.0)
                if latest >= t1 - 30 or attempt == attempts - 1:
                    break
                time.sleep(wait_s)  # the newest usage can take a short while to appear
        except Exception as exc:
            return None, error_text(exc)
        finally:
            close_quietly(c)
        hours = cost = 0.0
        cores = []
        for start, end, n, ch, usd in spans:
            overlap = min(end, t1) - max(start, t0)
            if overlap <= 0 or end <= start:
                continue
            share = overlap / (end - start)
            hours += ch * share
            cost += usd * share
            cores.append(n)
        if not cores:
            return None, f"no usage rows for cluster {self.cluster} during the run in {USAGE_VIEW}"
        return {"core_hours": hours, "cost": cost, "cores_min": min(cores), "cores_max": max(cores),
                "complete": latest >= t1 - 30, "recorded_to": latest}, None

    def finish(self, results, t0, t1):
        facts = [("e6data_dataset", f"{self.catalog}.{self.schema}"), ("e6data_cluster", self.cluster)]
        notes, lines = [], []
        ok = [r for r in results if r["status"] == "OK"]
        if ok and not self.args.no_server_stats:
            notes.append("fetching e6data server timings adds a short call after each query, which lowers "
                         "throughput; use --no-server-stats for throughput runs")
        if not results or self.args.e6_no_usage:
            return facts, notes, lines
        print(f"\nreading e6data usage and cost for this run from {USAGE_VIEW} ...", flush=True)
        usage, err = self.usage(t0, t1)
        if err:
            notes.append(f"could not read e6data usage and cost: {err}")
            return facts, notes, lines
        facts += [("e6data_core_hours", round(usage["core_hours"], 4)), ("e6data_cost_usd", round(usage["cost"], 4))]
        span = (f"{usage['cores_min']:g} cores" if usage["cores_min"] == usage["cores_max"]
                else f"{usage['cores_min']:g} to {usage['cores_max']:g} cores")
        lines += [f"\ne6data usage during this run (cluster {self.cluster}, {span}): "
                  f"{usage['core_hours']:.2f} core-hours, cost ${usage['cost']:.2f}",
                  f"  from {USAGE_VIEW}; this is the whole cluster's usage while the run was going, "
                  "including anyone else's queries on it at the same time"]
        if not usage["complete"]:
            gap = max(1, round((t1 - usage["recorded_to"]) / 60))
            lines.append(f"  warning: usage for the last ~{gap} min of the run is not recorded yet, so this "
                         "figure is low. Rerun the usage SQL in docs/engines/e6data.md in about 10 minutes "
                         "for the full number.")
        return facts, notes, lines
