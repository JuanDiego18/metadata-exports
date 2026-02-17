"""
Export Snowflake object definitions into individual .sql files for the Databricks
Lakebridge Analyzer. Layout:
  <out>/ddl/           - DDL (tables, views, functions, procedures, etc.) by type and db.schema
  <out>/workloads/queries/ - Runtime SQL from ACCOUNT_USAGE.query_history (optional)
One artifact per file; DDL and workload SQL are in separate root folders.
"""
import os
import re
import sys
import argparse
import pathlib
import datetime as dt
from typing import List, Dict, Tuple, Optional

# Check if snowflake-connector-python is installed. 
try:
    import snowflake.connector as sf
except ImportError:
    print("Please install snowflake-connector-python: pip install snowflake-connector-python")
    sys.exit(1)

SYSTEM_DATABASES = {"SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"}
SYSTEM_SCHEMAS = {"INFORMATION_SCHEMA"}

OBJECT_EXPORTS = [
    # name of SHOW command, GET_DDL type, subfolder
    ("SHOW TABLES IN {db}.{schema}", "TABLE", "tables"),
    ("SHOW VIEWS IN {db}.{schema}", "VIEW", "views"),
    ("SHOW MATERIALIZED VIEWS IN {db}.{schema}", "MATERIALIZED VIEW", "materialized_views"),
    ("SHOW SEQUENCES IN {db}.{schema}", "SEQUENCE", "sequences"),
    ("SHOW STAGES IN {db}.{schema}", "STAGE", "stages"),
    ("SHOW FILE FORMATS IN {db}.{schema}", "FILE FORMAT", "file_formats"),
    ("SHOW STREAMS IN {db}.{schema}", "STREAM", "streams"),
    ("SHOW TASKS IN {db}.{schema}", "TASK", "tasks"),
    ("SHOW PIPES IN {db}.{schema}", "PIPE", "pipes"),
    ("SHOW MASKING POLICIES IN {db}.{schema}", "MASKING POLICY", "masking_policies"),
    ("SHOW ROW ACCESS POLICIES IN {db}.{schema}", "ROW ACCESS POLICY", "row_access_policies"),
    ("SHOW TAGS IN {db}.{schema}", "TAG", "tags"),
]

# Functions and procedures need special handling (signature)
# Use SHOW USER PROCEDURES so we only get user-defined procedures; SHOW PROCEDURES
# includes Snowflake system/built-in procedures (e.g. SYSTEM$*, Cortex) which GET_DDL cannot export.
SHOW_FUNCTIONS_SQL = "SHOW USER FUNCTIONS IN SCHEMA {db}.{schema}"
SHOW_PROCEDURES_SQL = "SHOW USER PROCEDURES IN SCHEMA {db}.{schema}"

def quote_ident(name: str) -> str:
    # Always double-quote to preserve case and special chars
    q = name.replace('"', '""')
    return f'"{q}"'

def fq_name(db: str, schema: str, name: str) -> str:
    return f"{quote_ident(db)}.{quote_ident(schema)}.{quote_ident(name)}"

def row_to_dict(cursor, row) -> Dict[str, str]:
    cols = [d[0].lower() for d in cursor.description]
    return {c: (str(v) if v is not None else "") for c, v in zip(cols, row)}

def ensure_dir(p: pathlib.Path):
    p.mkdir(parents=True, exist_ok=True)


def safe_filename_part(s: str, max_len: int = 120) -> str:
    """Make a string safe for use in filenames: remove path chars, truncate."""
    s = re.sub(r'[<>:"/\\|?*\s]', "_", s)
    s = s.strip("._") or "unnamed"
    return s[:max_len] if len(s) > max_len else s

def write_sql(out_dir: pathlib.Path, fn: str, sql: str):
    p = out_dir / fn
    ensure_dir(p.parent)
    p.write_text(sql, encoding="utf-8")

def connect(args):
    password = os.environ.get(args.password_env) if args.password_env else args.password
    if not password:
        raise RuntimeError("Snowflake password not provided (use --password or --password-env).")
    return sf.connect(
        user=args.user,
        password=password,
        account=args.account,
        role=args.role,
        warehouse=args.warehouse,
        session_parameters={'QUERY_TAG': 'lakebridge_export'}
    )

def list_databases(cur, explicit_dbs: Optional[List[str]]) -> List[str]:
    if explicit_dbs:
        return explicit_dbs
    cur.execute("SHOW DATABASES")
    dbs = []
    for row in cur.fetchall():
        r = row_to_dict(cur, row)
        dbname = r.get("name", "")
        if dbname and dbname.upper() not in SYSTEM_DATABASES:
            dbs.append(dbname)
    return dbs

def list_schemas(cur, db: str, explicit_schemas: Optional[List[str]]) -> List[str]:
    if explicit_schemas:
        return explicit_schemas
    cur.execute(f"SHOW SCHEMAS IN {quote_ident(db)}")
    schemas = []
    for row in cur.fetchall():
        r = row_to_dict(cur, row)
        sname = r.get("name", "")
        if sname and sname.upper() not in SYSTEM_SCHEMAS:
            schemas.append(sname)
    return schemas

def escape_sql_string(s: str) -> str:
    """Escape single quotes for use inside a SQL string literal."""
    return s.replace("'", "''")


def safe_get_ddl(cur, obj_type: str, fqname: str) -> Optional[str]:
    """Get DDL for object; fqname should be the unquoted fully-qualified identifier."""
    try:
        # Pass as string literal to avoid injection; escape single quotes in identifier
        safe_literal = "'" + escape_sql_string(fqname) + "'"
        cur.execute(f"SELECT GET_DDL('{obj_type}', {safe_literal}, true)")
        ddl = cur.fetchone()
        if ddl and ddl[0]:
            return ddl[0]
    except Exception as e:
        print(f"[WARN] GET_DDL failed for {obj_type} {fqname}: {e}")
    return None

def _is_timestamp_like_table_name(name: str) -> bool:
    """True if name looks like a timestamp (e.g. '2026-01-29 23:56:22.491000-08:00'). Such tables often fail GET_DDL."""
    if not name or len(name) < 10:
        return False
    return name[0].isdigit() and " " in name and ":" in name

def export_simple_objects(cur, out_base: pathlib.Path, db: str, schema: str):
    for show_sql, ddl_type, subfolder in OBJECT_EXPORTS:
        sql = show_sql.format(db=quote_ident(db), schema=quote_ident(schema))
        try:
            cur.execute(sql)
        except Exception as e:
            print(f"[WARN] SHOW failed: {sql} -> {e}")
            continue
        out_dir = out_base / "ddl" / subfolder / f"{safe_filename_part(db)}.{safe_filename_part(schema)}"
        ensure_dir(out_dir)
        for row in cur.fetchall():
            r = row_to_dict(cur, row)
            name = r.get("name") or r.get("tag_name") or r.get("stage_name") or r.get("file_format_name") or ""
            if not name:
                # Fallback to first column if name not found
                if cur.description and len(cur.description) > 0:
                    name = str(row[0])
            if ddl_type == "TABLE" and _is_timestamp_like_table_name(name):
                continue  # Skip timestamp-named tables that typically fail GET_DDL
            fq = fq_name(db, schema, name)
            ddl = safe_get_ddl(cur, ddl_type, fq)
            if ddl:
                fn = f"{safe_filename_part(db)}.{safe_filename_part(schema)}.{safe_filename_part(name)}.sql"
                write_sql(out_dir, fn, ddl)

def export_functions(cur, out_base: pathlib.Path, db: str, schema: str):
    out_dir = out_base / "ddl" / "functions" / f"{safe_filename_part(db)}.{safe_filename_part(schema)}"
    ensure_dir(out_dir)
    sql = SHOW_FUNCTIONS_SQL.format(db=quote_ident(db), schema=quote_ident(schema))
    try:
        cur.execute(sql)
    except Exception as e:
        print(f"[WARN] SHOW USER FUNCTIONS failed: {sql} -> {e}")
        return
    for row in cur.fetchall():
        r = row_to_dict(cur, row)
        name = r.get("name", "")
        args = r.get("arguments", "").strip()
        sig = f"({args})" if args else "()"
        fqsig = f"{quote_ident(db)}.{quote_ident(schema)}.{quote_ident(name)}{sig}"
        ddl = safe_get_ddl(cur, "FUNCTION", fqsig)
        if ddl:
            args_part = "_" + safe_filename_part(args.replace(" ", "").replace(",", "_")) if args else ""
            fn = f"{safe_filename_part(db)}.{safe_filename_part(schema)}.{safe_filename_part(name)}{args_part}.sql"
            write_sql(out_dir, fn, ddl)

def export_procedures(cur, out_base: pathlib.Path, db: str, schema: str):
    out_dir = out_base / "ddl" / "procedures" / f"{safe_filename_part(db)}.{safe_filename_part(schema)}"
    ensure_dir(out_dir)
    sql = SHOW_PROCEDURES_SQL.format(db=quote_ident(db), schema=quote_ident(schema))
    try:
        cur.execute(sql)
    except Exception as e:
        print(f"[WARN] SHOW USER PROCEDURES failed: {sql} -> {e}")
        return
    for row in cur.fetchall():
        r = row_to_dict(cur, row)
        name = r.get("name", "")
        # Skip Snowflake system/built-in procedures (GET_DDL does not support them)
        if name.startswith("SYSTEM$"):
            continue
        args = r.get("arguments", "").strip()
        sig = f"({args})" if args else "()"
        fqsig = f"{quote_ident(db)}.{quote_ident(schema)}.{quote_ident(name)}{sig}"
        ddl = safe_get_ddl(cur, "PROCEDURE", fqsig)
        if ddl:
            args_part = "_" + safe_filename_part(args.replace(" ", "").replace(",", "_")) if args else ""
            fn = f"{safe_filename_part(db)}.{safe_filename_part(schema)}.{safe_filename_part(name)}{args_part}.sql"
            write_sql(out_dir, fn, ddl)

def export_queries(
    cur, out_base: pathlib.Path, dbs: List[str], days: int, split_statements: bool = False
):
    """Export workload SQL from query_history. If split_statements is True, split each
    query_text by semicolon and write one statement per file (Lakebridge recommendation)."""
    out_dir = out_base / "workloads" / "queries"
    ensure_dir(out_dir)
    since = f"DATEADD('day', -{days}, CURRENT_TIMESTAMP())"
    # Restrict to selected databases and non-null query_text (escape single quotes in db names)
    db_filter = " OR ".join([f"database_name = '{escape_sql_string(db)}'" for db in dbs])
    where = f"start_time >= {since} AND query_text IS NOT NULL"
    if db_filter:
        where = f"{where} AND ({db_filter})"
    sql = f"""
    SELECT query_text, database_name, schema_name, start_time
    FROM snowflake.account_usage.query_history
    WHERE {where}
    ORDER BY start_time
    """
    try:
        cur.execute(sql)
    except Exception as e:
        print(f"[WARN] Query history export failed: {e}")
        return
    rows = cur.fetchall()
    print(f"[INFO] Exporting {len(rows)} queries from ACCOUNT_USAGE.query_history...")
    file_count = 0
    for i, (qt, db, sch, ts) in enumerate(rows, start=1):
        db = db or "NA"
        sch = sch or "NA"
        ts_part = ts.strftime("%Y%m%d_%H%M%S")
        if split_statements and qt:
            statements = [s.strip() for s in qt.split(";") if s.strip()]
            for j, stmt in enumerate(statements):
                file_count += 1
                fn = f"{ts_part}_{safe_filename_part(db)}_{safe_filename_part(sch)}_{i}_{j+1}.sql"
                write_sql(out_dir, fn, stmt + "\n")
        else:
            file_count += 1
            fn = f"{ts_part}_{safe_filename_part(db)}_{safe_filename_part(sch)}_{i}.sql"
            write_sql(out_dir, fn, qt)
    print(f"[INFO] Wrote {file_count} workload SQL file(s).")

def parse_args():
    ap = argparse.ArgumentParser(description="Export Snowflake code for Lakebridge Analyzer")
    ap.add_argument("--account", required=True)
    ap.add_argument("--user", required=True)
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--password", help="Snowflake password")
    group.add_argument("--password-env", help="Env var containing Snowflake password, e.g., SNOWFLAKE_PASSWORD")
    ap.add_argument("--role", required=True)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--databases", help="Comma-separated list of databases to include")
    ap.add_argument("--schemas", help="Comma-separated list of schemas to include")
    ap.add_argument("--out", default="snowflake_input", help="Output base folder")
    ap.add_argument("--export-queries", action="store_true", help="Export recent workload SQL from ACCOUNT_USAGE.query_history")
    ap.add_argument("--query-days", type=int, default=90, help="Days of query history to export (default 90)")
    ap.add_argument("--split-statements", action="store_true", help="Split workload query_text by ';' and write one statement per file (recommended for Lakebridge)")
    return ap.parse_args()

def main():
    args = parse_args()
    out_base = pathlib.Path(args.out).resolve()
    ensure_dir(out_base)

    explicit_dbs = [d.strip() for d in args.databases.split(",")] if args.databases else None
    explicit_schemas = [s.strip() for s in args.schemas.split(",")] if args.schemas else None

    conn = connect(args)
    cur = conn.cursor()
    try:
        dbs = list_databases(cur, explicit_dbs)
        print(f"[INFO] Databases: {dbs}")
        for db in dbs:
            schemas = list_schemas(cur, db, explicit_schemas)
            print(f"[INFO] {db} schemas: {schemas}")
            for schema in schemas:
                print(f"[INFO] Exporting objects from {db}.{schema} ...")
                export_simple_objects(cur, out_base, db, schema)
                export_functions(cur, out_base, db, schema)
                export_procedures(cur, out_base, db, schema)
        if args.export_queries:
            export_queries(cur, out_base, dbs, args.query_days, split_statements=args.split_statements)
        print(f"[INFO] Export complete. Output at: {out_base}")
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
