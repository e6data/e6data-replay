"""Read the queries CSV: any CSV with a column of SQL (found by content, or --sql-column)
and an optional column of query names (header containing name/id, or --name-column).
Comma, semicolon or tab separated."""
import csv
import io
import re
import sys

# Column headers are matched by pattern, not exact name. The SQL column is found by its
# content (the column whose cells read as SQL); these patterns only break ties.
SQL_HINT = re.compile(r"sql|query|statement|text|engine", re.I)
NAME_HINT = re.compile(r"name|id|label|key|title", re.I)
SQL_START = re.compile(r"\(|(select|with|show|describe|desc|explain|values|set|use|insert|create|drop|alter)\b")


def _is_comment_only(sql):
    lines = [l.strip() for l in sql.splitlines() if l.strip()]
    return all(l.startswith("--") for l in lines)


def _looks_like_sql(cell):
    """True if the cell's first non-comment line starts like a SQL statement."""
    for line in cell.strip().lstrip('"').splitlines():
        line = line.strip().lower()
        if line and not line.startswith("--"):
            return bool(SQL_START.match(line))
    return False


def _pick_column(header, wanted, flag):
    """Resolve a --sql-column / --name-column value: a header (any case) or a 1-based number."""
    low = [h.strip().lower() for h in header]
    if wanted.strip().lower() in low:
        return low.index(wanted.strip().lower())
    if wanted.isdigit() and 1 <= int(wanted) <= len(header):
        return int(wanted) - 1
    sys.exit(f"{flag} '{wanted}' is not a column of this CSV. Columns: {', '.join(header)}")


def read_queries(path, sql_column=None, name_column=None):
    try:
        raw = open(path, "rb").read()
    except OSError as exc:
        sys.exit(f"cannot open {path}: {exc.strerror}")
    for enc in ("utf-8-sig", "cp1252"):
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        sys.exit(f"cannot read {path}: save it as UTF-8 CSV")

    first = next((l for l in text.splitlines() if l.strip()), "")
    if not first:
        sys.exit("the CSV has no queries")
    # The separator is taken from the first line only (column names never contain
    # commas), so commas inside SQL can never be mistaken for column breaks.
    counts = {d: first.count(d) for d in (",", ";", "\t")}
    delim = max(counts, key=counts.get) if max(counts.values()) else None
    if delim is None or _looks_like_sql(first):
        # One column: every row is a whole SQL statement (quoted multi-line SQL still works).
        has_header = not _looks_like_sql(first)
        delim = None
        rows = [r for r in csv.reader(io.StringIO(text), delimiter="\x1f") if r and r[0].strip()]
        if has_header:
            rows = rows[1:]
        sql_col, name_col, ncols = 0, None, 1
    else:
        rows = [r for r in csv.reader(io.StringIO(text), delimiter=delim) if any(c.strip() for c in r)]
        has_header = not any(_looks_like_sql(c) for c in rows[0])
        # Without a header, the narrowest row gives the column count; wider rows had
        # unquoted separators inside their SQL and are rejoined below.
        ncols = len(rows[0]) if has_header else min(len(r) for r in rows)
        header = [h.strip() for h in rows[0]] if has_header else [f"column {i + 1}" for i in range(ncols)]
        data = rows[1:] if has_header else rows
        if not data:
            sys.exit("the CSV has no queries")
        sample = data[:500]

        def cells(i):
            return [r[i] for r in sample if len(r) > i and r[i].strip()]

        if sql_column:
            sql_col = _pick_column(header, sql_column, "--sql-column")
            sql_like = [sql_col]
        else:
            score = {i: (sum(map(_looks_like_sql, cells(i))) / len(cells(i)) if cells(i) else 0.0)
                     for i in range(len(header))}
            sql_like = [i for i in range(len(header)) if score[i] >= 0.5]
            if not sql_like:
                sys.exit("could not find a column of SQL queries (cells starting with SELECT, WITH, ...). "
                         f"Columns: {', '.join(header)}. Name it with --sql-column")
            sql_like.sort(key=lambda i: (-score[i], not SQL_HINT.search(header[i]), i))
            sql_col = sql_like[0]
        if name_column:
            name_col = _pick_column(header, name_column, "--name-column")
        else:
            others = [i for i in range(len(header)) if i not in sql_like]
            hinted = [i for i in others if NAME_HINT.search(header[i])] if has_header else []
            short = [i for i in others if cells(i) and all(len(c) <= 100 for c in cells(i))
                     and len(set(cells(i))) >= 0.9 * len(cells(i))]
            name_col = (hinted or short or [None])[0]
        note = f"reading SQL from column '{header[sql_col]}'"
        note += f", query names from '{header[name_col]}'" if name_col is not None else ", no name column"
        extra = [header[i] for i in sql_like[1:]]
        if extra:
            note += f" (also SQL-like: {', '.join(extra)}; pick another with --sql-column)"
        print(note)
        rows = data

    out, used = [], {}
    for line_no, r in enumerate(rows, 2 if has_header else 1):
        if len(r) > ncols:
            # An unquoted separator inside the SQL split it into extra fields.
            if sql_col == ncols - 1:
                r = r[:sql_col] + [delim.join(r[sql_col:])]
            else:
                sys.exit(f"row {line_no}: more fields than the header. Put the SQL in double quotes.")
        if len(r) <= sql_col or not r[sql_col].strip():
            continue
        sql = r[sql_col].strip().rstrip(";").strip()
        if not sql or _is_comment_only(sql):
            continue
        name = r[name_col].strip() if name_col is not None and len(r) > name_col else ""
        name = name or f"q{len(out) + 1:03d}"
        if name in used:
            n = used[name] + 1
            while f"{name}#{n}" in used:
                n += 1
            used[name] = n
            new = f"{name}#{n}"
            used[new] = 1
            print(f"note: query name '{name}' is used more than once; this one is reported as '{new}'")
            name = new
        else:
            used[name] = 1
        out.append((name, sql))
    if not out:
        sys.exit("the CSV has no queries")
    return out
