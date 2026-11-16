# Adding an engine

An engine is one Python class in `replay/engines/`. The core handles everything else: reading the CSV, the schedule, connections per engine, Ctrl+C, the results file and the summary.

## Steps

1. Create `replay/engines/<name>.py` with a subclass of `Engine` from `replay/engines/base.py`.
2. List it in `ENGINES` in `replay/engines/__init__.py`.
3. Add a page `docs/engines/<name>.md` (setup, options, what it measures), and a row in the README's engine table.
4. Check it: `python -m replay examples/queries.csv --engine <name> --runs 2`.

## The interface

Only `name` and `connect` are required. `base.py` documents each method.

| Member | Required | What it does |
|---|---|---|
| `name` | yes | Value for `--engine` and for the `engine` column |
| `connect()` | yes | Returns `(connection, None)` or `(None, "error message")`. The connection needs `cursor()` and `close()`. The cursor needs `execute(sql)`, `fetchall()` and `close()`, which is what most Python database connectors (PEP 249) provide |
| `add_arguments(group)` | no | Adds the engine's own options. Prefix them, for example `--dbr-host` |
| `__init__(args)` | no | Reads and checks settings from options and environment variables. Exits with a clear message if one is missing. Runs only when the engine is chosen |
| `describe()` | no | One line on where the queries run, printed before the run |
| `server_fields` | no | Which result columns the engine fills: `server_total_ms`, `server_execution_ms`, `server_queue_ms`, `server_planning_ms`, `server_compile_ms`, `server_rows_read`, `cached` |
| `query_stats(cursor)` | no | Server timings for the query just run, read after the client timer stops. Use this if the connector can return them right away (e6data does) |
| `finish(results, t0, t1)` | no | Runs after the run. Can fill server timings into the results in one batch (Databricks reads its query history here), and returns summary facts, notes and report lines (e6data reports cost here) |
| `cancel(cursor)` | no | Stops a running query on Ctrl+C. Default: `cursor.cancel()` |
| `query_id(cursor, error)` | no | The engine's id for the query. Default: `cursor.query_id` |
| `prepare(login_seconds)` | no | Waits until the engine is ready to be timed, after the warm-up query |
| `error_hint(error)` | no | Advice printed once when an error message calls for it |
| `connect_note` | no | Printed while logging in, for example when a stopped warehouse is started first |

## A minimal engine

This example uses a connector that follows PEP 249 (`connect`, `cursor`, `execute`, `fetchall`). Replace `someconnector` and the settings with your engine's.

```python
import os
import sys

from .base import Engine, error_text, with_deadline


class MyEngine(Engine):
    name = "myengine"

    @classmethod
    def add_arguments(cls, g):
        g.add_argument("--my-host", help="host (default: MY_HOST)")

    def __init__(self, args):
        super().__init__(args)
        self.host = args.my_host or os.environ.get("MY_HOST", "")
        self.token = os.environ.get("MY_TOKEN", "")
        if not (self.host and self.token):
            sys.exit("set MY_HOST and MY_TOKEN (see docs/engines/myengine.md)")

    def describe(self):
        return f"host {self.host}"

    def connect(self):
        try:
            import someconnector
        except ImportError:
            return None, "the connector is not installed: pip install someconnector"
        conn, exc, timed_out = with_deadline(
            lambda: someconnector.connect(host=self.host, token=self.token), 300)
        if timed_out:
            return None, f"no answer from {self.host} within 300s"
        if exc is not None:
            return None, f"login failed: {error_text(exc)}"
        return conn, None
```

Then in `replay/engines/__init__.py`:

```python
from .myengine import MyEngine

ENGINES = {cls.name: cls for cls in (E6Data, Databricks, MyEngine)}
```

The two included engines show the optional parts: `e6data.py` reads server timings per query and reports cost after the run. `databricks.py` reads server timings in one batch after the run.
