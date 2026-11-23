"""e6data over its HTTP REST API (HTTP/1.1), used by `--e6-transport http`.

For networks where gRPC cannot get through: gRPC needs HTTP/2, and some proxies that inspect
HTTPS (for example Zscaler) only pass HTTP/1.1. Uses only the Python standard library.

The connection and cursor mimic the parts of e6data-python-connector that replay uses
(cursor, execute, fetchall, query_id, cancel, explain_analyse, close), so the e6data engine
works the same with either transport.

Proxy: honours https_proxy / no_proxy. Certificates: the system store (or SSL_CERT_FILE),
plus E6_CA_BUNDLE if set (for example a proxy's root certificate).
"""
import base64
import http.client
import json
import os
import ssl
import sys
import threading
import time
import urllib.parse
import urllib.request

# The server holds the submit open up to WAIT_TIMEOUT, then the client polls. Kept short so a
# Ctrl+C cancel (which needs the query id from the submit reply) reaches a long query quickly.
WAIT_TIMEOUT = "10s"
REQUEST_TIMEOUT_S = 600      # socket timeout for one request; a login can take minutes while a cluster resumes
POLL_FIRST_S, POLL_MAX_S = 0.05, 0.25
RETRY_STATUSES = (502, 503, 504)   # e.g. while a suspended cluster resumes
RETRY_FOR_S = 400
IDLE_REOPEN_S = 20           # proxies and load balancers drop idle connections; reopen instead of reusing
DROPPED = (http.client.RemoteDisconnected, http.client.CannotSendRequest, ConnectionResetError,
           ConnectionAbortedError, BrokenPipeError, ssl.SSLEOFError)
DEBUG = bool(os.environ.get("E6_HTTP_DEBUG"))


class E6HttpError(Exception):
    """An error from the e6data HTTP API; `query_id` is set when the server returned one."""

    def __init__(self, message, query_id=""):
        super().__init__(message)
        self.query_id = query_id


def _ssl_context():
    ctx = ssl.create_default_context()
    extra = os.environ.get("E6_CA_BUNDLE", "").strip()
    if extra:
        ctx.load_verify_locations(cafile=extra)
    return ctx


def _proxy_for(host):
    """(proxy_host, proxy_port, proxy_auth_header) from https_proxy, or None."""
    if urllib.request.proxy_bypass(host):
        return None
    url = urllib.request.getproxies().get("https")
    if not url:
        return None
    p = urllib.parse.urlsplit(url if "://" in url else "http://" + url)
    auth = None
    if p.username:
        cred = f"{urllib.parse.unquote(p.username)}:{urllib.parse.unquote(p.password or '')}"
        auth = "Basic " + base64.b64encode(cred.encode()).decode()
    return p.hostname, p.port or (443 if p.scheme == "https" else 80), auth


def _error_message(body, fallback):
    """The server's error text from a JSON reply, else the raw text."""
    try:
        d = json.loads(body)
    except ValueError:
        return (body or fallback).strip()[:1000]
    st = d.get("status") if isinstance(d.get("status"), dict) else {}
    err = st.get("error") or d.get("error") or d.get("message") or d.get("errorMessage")
    if isinstance(err, dict):
        err = err.get("message") or json.dumps(err)
    return str(err or body or fallback)[:1000]


class _Http:
    """One keep-alive HTTPS connection to the workspace (through the proxy if one is set)."""

    def __init__(self, host, port):
        self.host, self.port = host, port
        self.ctx = _ssl_context()
        self.proxy = _proxy_for(host)
        self.conn = None
        self.last_used = 0.0

    def _open(self):
        if self.proxy:
            ph, pp, auth = self.proxy
            c = http.client.HTTPSConnection(ph, pp, timeout=REQUEST_TIMEOUT_S, context=self.ctx)
            c.set_tunnel(self.host, self.port, headers={"Proxy-Authorization": auth} if auth else None)
        else:
            c = http.client.HTTPSConnection(self.host, self.port, timeout=REQUEST_TIMEOUT_S, context=self.ctx)
        return c

    def request(self, method, path, headers, body=None, resend_ok=True):
        """(status, text). A connection idle for a while is reopened first. If the connection drops,
        the request is sent once more only when resend_ok (never for a query submit, which could
        otherwise run twice)."""
        data = json.dumps(body).encode() if body is not None else None
        if self.conn is not None and time.monotonic() - self.last_used > IDLE_REOPEN_S:
            self.close()
        for attempt in (1, 2):
            if self.conn is None:
                self.conn = self._open()
            try:
                self.conn.request(method, path, body=data, headers=headers)
                r = self.conn.getresponse()
                text = r.read().decode("utf-8", "replace")
                self.last_used = time.monotonic()
                if (r.getheader("connection") or "").lower() == "close":
                    self.close()
                return r.status, text
            except DROPPED:
                self.close()
                if attempt == 2 or not resend_ok:
                    raise
            except Exception:
                self.close()
                raise

    def close(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None


class HttpConnection:
    """A logged-in e6data session over HTTP/1.1. Not shared between threads, except cancel()."""

    def __init__(self, host, port, user, token, cluster, catalog, schema):
        self.host, self.port = host, port
        self.user, self.token = user, token
        self.cluster, self.catalog, self.schema = cluster, catalog, schema
        self.http = _Http(host, port)
        self.session = None
        self.lock = threading.Lock()   # guards login/session: cancel() runs on another thread

    def _headers(self, auth=True, session_id=None):
        h = {"cluster-name": self.cluster, "Content-Type": "application/json", "Accept": "application/json"}
        if auth and self.session:
            h["Authorization"] = f"Bearer {self.session}"
        if session_id:
            h["X-Session-Id"] = session_id
        return h

    def login(self, deadline_s=RETRY_FOR_S, http=None):
        """Get a session token. Retries while the cluster is resuming (502/503/504)."""
        http = http or self.http
        end = time.time() + deadline_s
        with self.lock:
            while True:
                status, text = http.request("POST", "/api/v1/authenticate", self._headers(auth=False),
                                            {"user": self.user, "password": self.token})
                if status == 200:
                    try:
                        self.session = json.loads(text)["sessionId"]
                    except (ValueError, KeyError):
                        raise E6HttpError(f"login reply had no session: {text[:300]}")
                    return self.session
                if status in RETRY_STATUSES and time.time() < end:
                    time.sleep(5)
                    continue
                raise E6HttpError(f"HTTP {status}: {_error_message(text, 'login failed')}")

    def call(self, method, path, body=None, session_id=None, http=None, resend_ok=True, on_retry=None):
        """JSON reply of one API call. Logs in again once if the session has expired, and waits
        (up to RETRY_FOR_S) while the server answers 502/503/504, e.g. during a cluster resume.
        With resend_ok=False (a query submit) only 503 is retried: after a 502/504 from a gateway
        the query may already be running, and sending it again could run it twice."""
        http = http or self.http
        retry_on = RETRY_STATUSES if resend_ok else (503,)
        relogged, end, pause = False, time.time() + RETRY_FOR_S, 2.0
        while True:
            status, text = http.request(method, path, self._headers(session_id=session_id), body, resend_ok)
            if status == 401 and not relogged:
                relogged = True
                self.login(http=http)
                continue
            if status in retry_on and time.time() < end:
                if on_retry:
                    on_retry(status, pause)
                time.sleep(pause)
                pause = min(pause * 2, 15.0)
                continue
            if status != 200:
                raise E6HttpError(f"HTTP {status}: {_error_message(text, method + ' ' + path)}")
            try:
                return json.loads(text) if text.strip() else {}
            except ValueError:
                raise E6HttpError(f"unexpected reply to {method} {path}: {text[:300]}")

    def cursor(self):
        return HttpCursor(self)

    def close(self):
        self.http.close()


class HttpCursor:
    def __init__(self, conn):
        self.conn = conn
        self.query_id = None
        self.session_id = None
        self.polls = 0
        self._rows = []
        self._cancel_requested = False

    def execute(self, sql):
        self.query_id, self._rows, self.polls = None, [], 0
        waited = []
        # The submit is not resent if the connection drops after sending: it could run twice.
        d = self.conn.call("POST", "/api/v2/sql/statements",
                           {"statement": sql, "catalog": self.conn.catalog, "schema": self.conn.schema,
                            "wait_timeout": WAIT_TIMEOUT, "format": "JSON_ARRAY"}, resend_ok=False,
                           on_retry=lambda status, pause: waited.append(pause))
        self.query_id = d.get("query_id") or d.get("statement_id")
        self.session_id = d.get("session_id") or None
        sid = d.get("statement_id") or self.query_id
        if self._cancel_requested:   # Ctrl+C arrived while the submit was in flight
            self.cancel()
        wait = POLL_FIRST_S
        while _state(d) in ("PENDING", "RUNNING"):
            time.sleep(wait)
            wait = min(wait * 2, POLL_MAX_S)
            self.polls += 1
            d = self.conn.call("GET", f"/api/v2/sql/statements/{sid}", session_id=self.session_id,
                               on_retry=lambda status, pause: waited.append(pause))
        if DEBUG:
            print(f"[e6data http] {self.query_id} polls={self.polls}", file=sys.stderr, flush=True)
        if waited:  # visible, so a slow row is not mistaken for engine time
            print(f"note: e6data answered {'/'.join(map(str, RETRY_STATUSES))} (busy or resuming); query "
                  f"{self.query_id or ''} waited {sum(waited):.0f} s, inside its client_seconds",
                  file=sys.stderr, flush=True)
        state = _state(d)
        if state != "SUCCEEDED":
            msg = _error_message(json.dumps(d), state or "no state in reply")
            raise E6HttpError(f"{self.query_id or ''} {state}: {msg}".strip(), self.query_id or "")
        manifest = d.get("manifest") or {}
        result = d.get("result")
        if result is None:   # no inline rows: the status call also returns the first chunk
            again = self.conn.call("GET", f"/api/v2/sql/statements/{sid}", session_id=self.session_id)
            result, manifest = again.get("result"), again.get("manifest") or manifest
        rows = list(_data(result))
        chunks = int(manifest.get("total_chunk_count") or 1)
        for n in range(1, chunks):
            rows += list(_data(self.conn.call("GET", f"/api/v2/sql/statements/{sid}/result/chunks/{n}",
                                              session_id=self.session_id)))
        expected = manifest.get("total_row_count")
        if expected is not None and not manifest.get("truncated") and int(expected) != len(rows):
            raise E6HttpError(f"{self.query_id} got {len(rows)} rows, server reported {expected}", self.query_id)
        self._rows = rows

    def fetchall(self):
        return self._rows

    def explain_analyse(self):
        """Same keys as the connector's explain_analyse()."""
        e = self.conn.call("GET", f"/api/v2/sql/statements/{self.query_id}/explain-analyze",
                           session_id=self.session_id)
        plan = e.get("explainAnalyze")
        if isinstance(plan, (dict, list)):
            plan = json.dumps(plan)
        return {"is_cached": e.get("isCached"), "parsing_time": e.get("parsingTimeMs"),
                "queuing_time": e.get("queueingTimeMs"), "planner": plan}

    def cancel(self, query_id=None):
        """Called from another thread: uses its own HTTP connection. Before the submit has
        returned there is no id yet, so the cancel is sent as soon as it does."""
        self._cancel_requested = True   # set first: execute() checks it right after it learns the id
        qid = query_id or self.query_id
        if not qid:
            return
        side = _Http(self.conn.host, self.conn.port)
        try:
            self.conn.call("POST", f"/api/v2/sql/statements/{qid}/cancel", session_id=self.session_id, http=side)
        finally:
            side.close()

    def close(self):
        """Free the statement's resources on the server (best effort)."""
        if self.query_id:
            try:
                self.conn.call("POST", f"/api/v2/sql/statements/{self.query_id}/clearOrCancel",
                               session_id=self.session_id)
            except Exception:
                pass


def _data(chunk):
    chunk = chunk or {}
    return chunk.get("data_array") or (chunk.get("result") or {}).get("data_array") or []


def _state(d):
    st = d.get("status")
    return (st.get("state") if isinstance(st, dict) else st or "").upper()
