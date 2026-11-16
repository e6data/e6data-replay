"""The interface every engine implements.

An engine is one class. The core opens connections through it, sends each query on a
cursor (execute, then fetchall), and asks it for anything engine-specific: the
server's own timings, how to cancel a running query, what to do before the clock
starts and after the run ends. See docs/adding-an-engine.md.
"""
import threading

# Columns an engine can fill in the results file, in the order they are written.
# An engine lists the ones it fills in `server_fields`; the others stay out of the file.
SERVER_FIELDS = ("server_total_ms", "server_execution_ms", "server_queue_ms", "server_planning_ms",
                 "server_compile_ms", "server_rows_read", "cached")


def error_text(exc):
    """One line of error text, cut to 1,000 characters (gRPC errors keep it in details())."""
    details = getattr(exc, "details", None)
    try:
        msg = details() if callable(details) else None
    except Exception:
        msg = None
    return " ".join(str(msg or exc).split())[:1000]


def close_quietly(c):
    try:
        c.close()
    except Exception:
        pass


def with_deadline(fn, seconds):
    """Run fn() in a background thread. Returns (result, exception, timed_out).
    Logins go through this: a blocked network otherwise hangs silently inside a connector."""
    box = {}

    def go():
        try:
            box["res"] = fn()
        except Exception as exc:
            box["err"] = exc
    t = threading.Thread(target=go, daemon=True)
    t.start()
    t.join(seconds)
    if t.is_alive():
        return None, None, True
    return box.get("res"), box.get("err"), False


class Engine:
    """Base class. Subclasses set `name` and implement `connect`; the rest is optional."""

    name = ""              # value for --engine and for the `engine` column
    server_fields = ()     # which of SERVER_FIELDS this engine fills
    connect_note = ""      # printed while logging in, e.g. "a stopped warehouse is started first"

    @classmethod
    def add_arguments(cls, group):
        """Add this engine's own options (prefix them, e.g. --dbr-host) to an argparse group."""

    def __init__(self, args):
        """Read and check settings (options, environment variables). Exit with a clear
        message if something required is missing. Runs only for engines picked with --engine."""
        self.args = args

    def describe(self):
        """One line on where the queries run, printed before the run."""
        return ""

    def connect(self):
        """A new logged-in connection, or (None, error message). Returns (conn, None) on success.
        The connection needs cursor() and close(); the cursor needs execute(), fetchall() and close()."""
        raise NotImplementedError

    def cancel(self, cursor):
        """Stop the query running on this cursor (called from another thread on Ctrl+C)."""
        cursor.cancel()

    def query_stats(self, cursor):
        """The server's own timings for the query just run on this cursor, as a dict of
        server_fields, or None if they could not be read. Called after the client timer stops."""
        return None

    def query_id(self, cursor, error):
        """The engine's id for the query, to look it up in the engine's own query history."""
        return str(getattr(cursor, "query_id", "") or "")

    def error_hint(self, error):
        """Advice to print once if an error message calls for it, or None."""
        return None

    def prepare(self, login_seconds):
        """Before the clock starts, after the --warmup-sql query: wait until the engine is
        ready to be timed. Returns a warning to print, or None."""
        return None

    def finish(self, results, t0, t1):
        """After the run (epoch seconds t0..t1). May fill server timings into `results`
        (this engine's rows, as dicts). Returns (summary_facts, notes, lines):
        [(key, value)] for the summary file, notes to print, and report lines to print."""
        return [], [], []
