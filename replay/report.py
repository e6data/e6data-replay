"""Percentiles and the run summary file."""
import csv
import statistics

from .engines.base import SERVER_FIELDS


def quantile(values, q):
    """Linear interpolation between closest ranks (same as Excel PERCENTILE.INC)."""
    v = sorted(values)
    if len(v) == 1:
        return v[0]
    pos = q * (len(v) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (pos - lo)


def summary_path(out):
    base = out[:-4] if out.lower().endswith(".csv") else out
    return f"{base}_summary.csv"


def write_summary(out, engines, results, total, facts):
    """One row per percentile, one column per engine and time (ms), over successful
    executions; counts and run facts underneath. Returns its path, or None."""
    path = summary_path(out)

    def ms(r, key):
        v = r.get(key)
        return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None
    cols = []
    for eng in engines:
        ok = [r for r in results if r["status"] == "OK" and r["engine"] == eng.name]
        cols.append((f"{eng.name}_client_ms", [r["client_seconds"] * 1000 for r in ok]))
        for key in SERVER_FIELDS:
            if key in eng.server_fields and key.endswith("_ms"):
                cols.append((f"{eng.name}_{key}", [x for x in (ms(r, key) for r in ok) if x is not None]))

    stats = [(f"p{q}", lambda v, q=q: quantile(v, q / 100)) for q in (50, 75, 90, 99)] + \
            [("max", max), ("mean", statistics.mean), ("total", sum)]
    width = 1 + len(cols)
    rows = [["percentile"] + [name for name, _ in cols]]
    for label, fn in stats:
        rows.append([label] + [round(fn(v)) if v else "" for _, v in cols])

    counts = []
    for eng in engines:
        mine = [r for r in results if r["engine"] == eng.name]
        counts += [(f"{eng.name}_SUCCESS", sum(1 for r in mine if r["status"] == "OK")),
                   (f"{eng.name}_FAILED", sum(1 for r in mine if r["status"] == "FAILED")),
                   (f"{eng.name}_CANCELLED", sum(1 for r in mine if r["status"] == "CANCELLED")),
                   (f"{eng.name}_NOT_RUN", total - len(mine))]
    rows += [[k, v] + [""] * max(0, width - 2) for k, v in counts + list(facts)]
    try:
        with open(path, "w", newline="", encoding="utf-8-sig") as sf:
            csv.writer(sf).writerows(rows)
        return path
    except OSError as exc:
        print(f"warning: could not write {path}: {exc.strerror}")
        return None
