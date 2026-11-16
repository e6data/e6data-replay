"""Replay a CSV of SQL queries on one or more query engines and write the timings to a CSV.

    python -m replay queries.csv --engine e6data                       # one query at a time
    python -m replay queries.csv --engine e6data --runs 3
    python -m replay queries.csv --engine databricks --pattern examples/qps_pattern.json
    python -m replay queries.csv --engine e6data,databricks            # same queries, same schedule, on both

With more than one engine, every engine gets the same queries on the same schedule,
starting together; each engine has its own connections, so a slow one never holds up another.

Output: one row per execution with the time measured here and the engine's own server-side
timings, plus a <out>_summary.csv with percentiles. Exit code: 0 if every execution
succeeded, 1 otherwise. Each engine's settings: docs/engines/<engine>.md.
"""
import argparse
import csv
import json
import math
import os
import queue
import statistics
import sys
import threading
import time
from datetime import datetime

from .engines import ENGINES
from .engines.base import SERVER_FIELDS, close_quietly, error_text
from .queries import read_queries
from .report import quantile, write_summary

INTERRUPT_WAIT_S = 30
BASE_FIELDS = ["engine", "name", "run", "status", "started", "client_seconds", "rows"]
TAIL_FIELDS = ["query_id", "error"]

try:  # never let an odd character in an error message crash the console output
    sys.stdout.reconfigure(errors="replace")
except Exception:
    pass


def now_iso():
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


class Runner:
    """One engine during a run: its connections, its job queue and its counters."""

    def __init__(self, engine):
        self.engine = engine
        self.name = engine.name
        self.conns, self.pool, self.jobs = [], queue.Queue(), queue.Queue()
        self.login_s = 0.0
        self.arr = {"lags": [], "open": 0, "running": 0, "peak": 0, "open_at_end": None,
                    "running_at_end": None, "extra_logins": 0, "login_errors": 0}

    def open_many(self, n):
        """n ready connections before the clock starts: one first (it may wake a stopped
        warehouse or cluster), then the rest at once. Returns an error or None."""
        c, err = self.engine.connect()
        if err:
            return err
        self.conns.append(c)
        box = [None] * (n - 1)

        def go(i):
            box[i] = self.engine.connect()
        threads = [threading.Thread(target=go, args=(i,), daemon=True) for i in range(n - 1)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        for res in box:
            if res is None:
                return f"{self.name}: a login did not finish"
            c, err = res
            if err:
                return err
            self.conns.append(c)
        return None

    def close_all(self):
        for c in self.conns:
            if c is not None:
                close_quietly(c)


def run_one(conn, runner, name, run, worker, sql, inflight, stop, server_stats):
    eng = runner.engine
    rec = {"engine": eng.name, "name": name, "run": run, "worker": worker, "sql": sql, "query_id": "",
           "error": "", "rows": 0, "status": "FAILED", "client_seconds": 0.0, "started": now_iso()}
    t0 = time.perf_counter()
    cur = None
    try:
        cur = conn.cursor()
        inflight[(eng.name, worker)] = (eng, cur)
        cur.execute(sql)
        rows = cur.fetchall() or []
        rec["client_seconds"] = round(time.perf_counter() - t0, 3)
        rec.update(status="OK", rows=len(rows))
        if server_stats and eng.query_stats is not None:
            stats = eng.query_stats(cur)
            rec.update(stats if stats is not None else {"_stats_missing": True})
    except Exception as exc:
        rec["client_seconds"] = round(time.perf_counter() - t0, 3)
        rec["error"] = error_text(exc)
        if stop.is_set():
            rec["status"] = "CANCELLED"
    finally:
        inflight.pop((eng.name, worker), None)
    try:
        rec["query_id"] = eng.query_id(cur, rec["error"]) if cur is not None else eng.query_id(None, rec["error"])
    except Exception:
        pass
    if cur is not None:
        close_quietly(cur)
    return rec


def parse_args(argv=None):
    ap = argparse.ArgumentParser(prog="python -m replay", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("csv", help="input CSV of queries")
    ap.add_argument("--engine", required=True,
                    help=f"engine(s) to run on, comma separated: {', '.join(ENGINES)}")
    ap.add_argument("--runs", type=int, default=1, help="times each query is run (default 1)")
    ap.add_argument("--pattern",
                    help='load pattern: a JSON file (or JSON text) listing steps, e.g. '
                         '[{"Seconds": 10, "QPS": 2}, {"Seconds": 8, "QPS": 5}], run in order, cycling through the CSV')
    ap.add_argument("--max-inflight", type=int, default=64,
                    help="with --pattern: most queries open at once from this machine, per engine (default 64)")
    ap.add_argument("--warmup-sql", default="SELECT 1",
                    help="untimed query run once per engine before the clock starts (default: SELECT 1; "
                         "use a small table scan to wake the workers, or '' to skip)")
    ap.add_argument("--no-server-stats", action="store_true",
                    help="skip fetching the engines' server-side timings (use for throughput runs)")
    ap.add_argument("--sql-column", help="column holding the SQL, by header or 1-based number (default: found automatically)")
    ap.add_argument("--name-column", help="column holding query names, by header or 1-based number (default: found automatically)")
    ap.add_argument("--out", default=f"results_{time.strftime('%Y%m%d_%H%M%S')}.csv",
                    help="results CSV (default results_<date>_<time>.csv; an existing file is overwritten)")
    for cls in ENGINES.values():
        cls.add_arguments(ap.add_argument_group(f"{cls.name} options (docs/engines/{cls.name}.md)"))
    return ap.parse_args(argv)


def read_pattern(spec):
    """(start offsets in seconds, steps) from a --pattern file or JSON text."""
    try:
        text = open(spec, encoding="utf-8").read() if os.path.isfile(spec) else spec
        steps = [(float(st["Seconds"]), float(st["QPS"])) for st in json.loads(text)]
    except (OSError, ValueError, KeyError, TypeError) as exc:
        sys.exit(f"cannot read --pattern: {exc}")
    if not steps or any(sec <= 0 or rate <= 0 for sec, rate in steps):
        sys.exit("--pattern needs at least one step, each with Seconds and QPS above 0")
    offsets, t0 = [], 0.0
    for sec, rate in steps:
        offsets += [t0 + k / rate for k in range(max(1, round(sec * rate)))]
        t0 += sec
    return offsets, steps


def main(argv=None):
    a = parse_args(argv)
    if a.runs < 1:
        sys.exit("--runs must be 1 or more")
    if a.max_inflight < 1:
        sys.exit("--max-inflight must be 1 or more")
    names = list(dict.fromkeys(n.strip().lower() for n in a.engine.split(",") if n.strip()))
    unknown = [n for n in names if n not in ENGINES]
    if not names or unknown:
        sys.exit(f"unknown --engine {', '.join(unknown) or a.engine!r}; choose from: {', '.join(ENGINES)}")

    offsets = steps = None  # start time of each query, in seconds from the start (--pattern)
    if a.pattern is not None:
        if a.runs != 1:
            sys.exit("--runs does not apply with --pattern (the pattern sets how many queries start)")
        offsets, steps = read_pattern(a.pattern)
        duration = sum(sec for sec, _ in steps)
        avg_qps = len(offsets) / duration
        pattern_desc = ", ".join(f"{rate:g}/s x {sec:g}s" for sec, rate in steps)

    queries = read_queries(a.csv, a.sql_column, a.name_column)
    if os.path.abspath(a.out) == os.path.abspath(a.csv):
        sys.exit("--out is the same file as the input CSV; choose another name")
    engines = [ENGINES[n](a) for n in names]  # each reads and checks its own settings
    runners = [Runner(e) for e in engines]
    multi = len(runners) > 1
    served = {f for e in engines for f in e.server_fields}
    fields = BASE_FIELDS + [f for f in SERVER_FIELDS if f in served] + TAIL_FIELDS
    try:
        f = open(a.out, "w", newline="", encoding="utf-8-sig")
    except OSError as exc:
        sys.exit(f"cannot write {a.out}: {exc.strerror}")
    writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()

    if offsets is not None:
        joblist = [(queries[i % len(queries)][0], i // len(queries) + 1, queries[i % len(queries)][1])
                   for i in range(len(offsets))]
    else:
        joblist = [(name, run, sql) for run in range(1, a.runs + 1) for name, sql in queries]
    for r in runners:
        for j in joblist:
            r.jobs.put(j)
    total = len(joblist)
    w = max(14, max(len(n) for n, _ in queries))
    if offsets is not None:
        peak_rate = max(rate for _, rate in steps)
        workers = min(total, a.max_inflight, max(2, math.ceil(peak_rate * 4)))  # pre-logged-in pool; grows on demand
        mode = f"pattern {pattern_desc} ({duration:g} s)"
        print(f"{len(queries)} queries, {total} executions over {duration:g}s in steps ({pattern_desc})")
    else:
        workers = 1
        mode = f"{a.runs} run(s), one at a time"
        print(f"{len(queries)} queries x {a.runs} run(s) = {total} executions, one at a time")
    for r in runners:
        print(f"  {r.name}: {r.engine.describe()}")
    if multi:
        print("  every engine gets the same queries on the same schedule, starting together")

    # Connections are logged in before the clock starts.
    def close_all():
        for r in runners:
            r.close_all()
    for r in runners:
        print(f"logging in to {r.name} ({workers} connection(s)) ..."
              + (f" {r.engine.connect_note}" if r.engine.connect_note else ""), flush=True)
        login_t0 = time.time()
        try:
            err = r.open_many(workers)
        except KeyboardInterrupt:
            err = "interrupted while logging in; no queries were run"
        if err:
            close_all()
            f.close()
            sys.exit(err)
        r.login_s = time.time() - login_t0
        for i, c in enumerate(r.conns):
            r.pool.put((i + 1, c))

    for r in runners:  # untimed: wake the engine, then let it say when it is ready
        print(f"warming up {r.name} (not timed) ...", flush=True)
        if a.warmup_sql.strip():
            cur = None
            try:
                cur = r.conns[0].cursor()
                cur.execute(a.warmup_sql)
                cur.fetchall()
            except Exception as exc:
                print(f"  note: warm-up query failed: {error_text(exc)}")
            finally:
                if cur is not None:
                    close_quietly(cur)
        msg = r.engine.prepare(r.login_s)
        if msg:
            print(f"  note: {msg}")

    lock = threading.Lock()
    stop = threading.Event()
    inflight = {}
    results = []
    state = {"hinted": set(), "closed": False, "stats_missing": 0}
    grand_total = total * len(runners)
    server_stats = not a.no_server_stats

    def record(rec, r, wid, name, run):
        with lock:
            if state["closed"]:
                return
            if rec.pop("_stats_missing", False):
                state["stats_missing"] += 1
            results.append(rec)
            writer.writerow(rec)
            f.flush()
            extra = f"  server {rec['server_total_ms']} ms" if rec.get("server_total_ms") is not None else ""
            tag = f"{r.name:<10} " if multi else ""
            print(f"  [{len(results):>4}/{grand_total}] {tag}w{wid} {name:<{w}} run {run}  {rec['status']:<9} "
                  f"{rec['client_seconds']:>8.2f}s  rows={rec['rows']}{extra}"
                  + (f"  {rec['error'][:150]}" if rec["error"] else ""), flush=True)
            hint = r.engine.error_hint(rec["error"]) if rec["error"] else None
            if hint and r.name not in state["hinted"]:
                state["hinted"].add(r.name)
                print("  " + hint)

    def worker(r, wid, conn):
        while not stop.is_set():
            try:
                name, run, sql = r.jobs.get_nowait()
            except queue.Empty:
                return
            record(run_one(conn, r, name, run, wid, sql, inflight, stop, server_stats), r, wid, name, run)

    # --pattern: a pool of logged-in connections per engine, one thread per started query.
    def get_conn(r):
        try:
            return r.pool.get_nowait()
        except queue.Empty:
            pass
        with lock:
            grow = len(r.conns) < a.max_inflight
            if grow:
                r.conns.append(None)  # hold the slot while logging in
                cid = len(r.conns)
        if grow:
            c, err = r.engine.connect()
            if not err:
                with lock:
                    r.conns[cid - 1] = c
                r.arr["extra_logins"] += 1
                return cid, c
            r.arr["login_errors"] += 1
        return r.pool.get()  # at --max-inflight: wait for a connection to come back

    def arrival(r, name, run, sql, due):
        cid, conn = get_conn(r)
        if stop.is_set():  # Ctrl+C while waiting for a connection: don't start it
            r.pool.put((cid, conn))
            with lock:
                r.arr["open"] -= 1
            return
        lag = time.perf_counter() - due
        with lock:  # sent to the engine now; before this it was waiting for a connection here
            r.arr["running"] += 1
            r.arr["peak"] = max(r.arr["peak"], r.arr["running"])
        rec = run_one(conn, r, name, run, cid, sql, inflight, stop, server_stats)
        with lock:  # count it finished before its connection is handed to the next query
            r.arr["lags"].append(lag)
            r.arr["open"] -= 1
            r.arr["running"] -= 1
        r.pool.put((cid, conn))
        record(rec, r, cid, name, run)

    wall0 = time.perf_counter()
    run_t0 = time.time()
    interrupted = False
    threads = []
    try:
        if offsets is None:
            threads = [threading.Thread(target=worker, args=(r, i + 1, r.conns[i]), daemon=True)
                       for i in range(workers) for r in runners]
            for t in threads:
                t.start()
        else:
            for i, (name, run, sql) in enumerate(joblist):
                due = wall0 + offsets[i]
                while not stop.is_set() and time.perf_counter() < due:
                    time.sleep(min(0.05, max(0.0, due - time.perf_counter())))
                for r in runners:  # same query, same moment, on every engine
                    with lock:
                        r.arr["open"] += 1
                    t = threading.Thread(target=arrival, args=(r, name, run, sql, due), daemon=True)
                    threads.append(t)
                    t.start()
            with lock:
                for r in runners:
                    r.arr["open_at_end"] = r.arr["open"]
                    r.arr["running_at_end"] = r.arr["running"]
                    r.arr["send_s"] = time.perf_counter() - wall0
        while any(t.is_alive() for t in threads):
            for t in threads:
                t.join(0.5)
    except KeyboardInterrupt:
        interrupted = True
        stop.set()
        print(f"\ninterrupted: cancelling {len(inflight)} running queries (waiting up to {INTERRUPT_WAIT_S}s; "
              "press Ctrl+C again to quit at once) ...", flush=True)

        def cancel(eng, cur):
            try:
                eng.cancel(cur)
            except Exception:
                pass
        # All cancels at once, in the background: a busy engine can be slow to answer each one.
        for eng, cur in list(inflight.values()):
            threading.Thread(target=cancel, args=(eng, cur), daemon=True).start()
        deadline = time.monotonic() + INTERRUPT_WAIT_S
        try:
            for t in threads:
                t.join(max(0.0, deadline - time.monotonic()))
        except KeyboardInterrupt:
            print("quitting now; queries still running on the engines finish or time out there", flush=True)
            with lock:
                state["closed"] = True
                f.close()
            os._exit(1)
    wall = time.perf_counter() - wall0
    run_t1 = time.time()
    with lock:
        state["closed"] = True
        f.close()
    closer = threading.Thread(target=close_all, daemon=True)
    closer.start()
    closer.join(10)

    # After the run: each engine may add server timings, usage or cost.
    facts, notes, lines = [("mode", mode)], [], []
    for r in runners:
        mine = [rec for rec in results if rec["engine"] == r.name]
        try:
            fa, no, li = r.engine.finish(mine, run_t0, run_t1)
        except Exception as exc:
            fa, no, li = [], [f"{r.name}: after-run step failed: {error_text(exc)}"], []
        facts += fa
        notes += no
        lines += li
    try:  # rewrite the results with anything the engines added after the run
        with open(a.out, "w", newline="", encoding="utf-8-sig") as f2:
            wr = csv.DictWriter(f2, fieldnames=fields, extrasaction="ignore")
            wr.writeheader()
            wr.writerows(results)
    except OSError as exc:
        print(f"warning: could not rewrite {a.out}: {exc.strerror}")

    if offsets is not None:
        for r in runners:
            end = r.arr["open_at_end"] is not None
            facts += [(f"{r.name}_peak_running", r.arr["peak"]),
                      (f"{r.name}_running_when_arrivals_ended", r.arr["running_at_end"] if end else ""),
                      (f"{r.name}_waiting_when_arrivals_ended",
                       r.arr["open_at_end"] - r.arr["running_at_end"] if end else "")]
    failed = sum(1 for rec in results if rec["status"] != "OK")
    not_run = grand_total - len(results)
    summary = write_summary(a.out, engines, results, total, facts)
    print(f"\n{len(results) - failed} ok, {failed} failed/cancelled" + (f", {not_run} not run" if not_run else "")
          + f", wall time {wall:.1f}s -> {a.out}" + (f" and {summary}" if summary else ""))

    for r in runners:
        ok = [rec for rec in results if rec["status"] == "OK" and rec["engine"] == r.name]
        if not ok:
            continue
        print(f"\n{r.name}\n{'query':<{w}} {'ok runs':>7} {'median s':>9} {'min s':>7} {'max s':>7} {'server median s':>16}")
        for name, _ in queries:
            mine = [rec for rec in ok if rec["name"] == name]
            if not mine:
                print(f"{name:<{w}} {0:>7}")
                continue
            secs = [rec["client_seconds"] for rec in mine]
            srv = [rec["server_total_ms"] / 1000 for rec in mine
                   if isinstance(rec.get("server_total_ms"), (int, float))]
            print(f"{name:<{w}} {len(mine):>7} {statistics.median(secs):>9.2f} {min(secs):>7.2f} {max(secs):>7.2f} "
                  f"{(f'{statistics.median(srv):.2f}' if srv else '-'):>16}")
        if offsets is None:
            best = [min(rec["client_seconds"] for rec in ok if rec["name"] == name) for name, _ in queries
                    if any(rec["name"] == name for rec in ok)]
            print(f"{r.name} suite total (sum of each query's fastest run): {sum(best):.2f}s "
                  f"over {len(best)} of {len(queries)} queries")
        secs = [rec["client_seconds"] for rec in ok]
        print(f"{r.name} successful executions: throughput {len(ok) / wall:.2f} queries/s, "
              f"client latency p50 {quantile(secs, .5):.2f}s  p95 {quantile(secs, .95):.2f}s  "
              f"max {max(secs):.2f}s  mean {statistics.mean(secs):.2f}s")
        arr = r.arr
        if offsets is not None and arr["lags"]:
            lags = arr["lags"]
            print(f"{r.name} arrivals: target average {avg_qps:.3g} queries/s, "
                  f"{len(lags)} started over {arr.get('send_s', wall):.1f}s; "
                  f"start delay p50 {quantile(lags, .5) * 1000:.0f} ms, max {max(lags) * 1000:.0f} ms; "
                  f"peak running {arr['peak']} (--max-inflight {a.max_inflight})"
                  + ("" if arr["open_at_end"] is None else
                     f"; when arrivals ended: {arr['running_at_end']} running, "
                     f"{arr['open_at_end'] - arr['running_at_end']} waiting for a connection here"))
            if arr["extra_logins"]:
                print(f"note: {arr['extra_logins']} extra {r.name} connection(s) were opened during the run "
                      "(login time is not counted in client_seconds)")
            if max(lags) > 1:
                print(f"note: some {r.name} queries started more than 1 s late, so the target rate was not fully "
                      f"met: all {a.max_inflight} connections were busy, so it saw at most {a.max_inflight} "
                      "queries at once. Raise --max-inflight for a true arrival-rate test")
        cached = sum(1 for rec in ok if rec.get("cached"))
        if cached:
            print(f"note: {cached} {r.name} execution(s) were answered from the engine's result cache")
    if state["stats_missing"]:
        print(f"note: server timings could not be fetched for {state['stats_missing']} execution(s)")
    for line in lines:
        print(line)
    for n in notes:
        print(f"note: {n}")
    sys.exit(0 if failed == 0 and not_run == 0 and not interrupted else 1)
