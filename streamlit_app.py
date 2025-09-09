from __future__ import annotations

import io
import os
import re
import csv
import uuid
from dataclasses import dataclass
from datetime import datetime
import typing as t

import pandas as pd
import streamlit as st
import boto3
import botocore
import snowflake.connector

# ──────────────────────────────────────────────────────────────────────────────
# Acceptance Criteria Mapping (high level)
# 1) UI select reports → validate (discrepancy checks)                      ✅
# 2) Acceptable files → available for ingestion (checkbox)                  ✅
# 3) Submit → copy selected files to S3                                     ✅
# 4) Identified in S3 → ingest into Snowflake (COPY INTO)                   ✅
# 5) Ingested → corresponding table has all original columns & rows         ✅
# ──────────────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="Compliance Ingestion", page_icon="✅", layout="wide")
st.title("✅ Distributor Reports — Compliance Ingestion (Streamlit • S3 • Snowflake)")

# ──────────────────────────────────────────────────────────────────────────────
# Configuration via Streamlit Secrets
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class AppConfig:
    aws_region: str
    s3_bucket: str
    s3_prefix: str
    sf_account: str
    sf_user: str
    sf_password: str
    sf_role: str | None
    sf_wh: str
    sf_db: str
    sf_schema: str
    sf_stage: str

def load_cfg() -> AppConfig:
    aws = st.secrets["aws"]
    sf = st.secrets["snowflake"]
    return AppConfig(
        aws_region   = aws.get("region", "us-east-1"),
        s3_bucket    = aws["bucket"],
        s3_prefix    = aws.get("prefix", "ingestion").strip("/"),
        sf_account   = sf["account"],
        sf_user      = sf["user"],
        sf_password  = sf["password"],
        sf_role      = (sf.get("role") or "").strip() or None,
        sf_wh        = sf["warehouse"],
        sf_db        = sf["database"],
        sf_schema    = sf["schema"],
        sf_stage     = sf["stage"],
    )

CFG = load_cfg()

# Avoid leaking secrets in UI; only show non-sensitive summary.
with st.expander("Runtime Configuration (non-sensitive)", expanded=False):
    st.code(
        f"""AWS Region:         {CFG.aws_region}
S3 Bucket:          {CFG.s3_bucket}
S3 Prefix:          {CFG.s3_prefix}
Snowflake:          {CFG.sf_account}
Warehouse:          {CFG.sf_wh}
Database.Schema:    {CFG.sf_db}.{CFG.sf_schema}
Role:               {CFG.sf_role or '(default)'}
External Stage:     {CFG.sf_stage}
""",
        language="text",
    )

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

SAFE_CHAR_RE = re.compile(r"[^A-Z0-9_]")

def sanitize_identifier(name: str, default: str = "COL") -> str:
    """Snowflake-safe UPPERCASE identifier with A–Z, 0–9, _ ; no leading digit."""
    s = re.sub(r"\s+", "_", str(name or "").strip()).upper()
    s = SAFE_CHAR_RE.sub("_", s).strip("_")
    if not s:
        s = default
    if s[0].isdigit():
        s = f"_{s}"
    return s

def ensure_unique(names: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    out: list[str] = []
    for n in names:
        base = n
        i = seen.get(base, 0)
        if i == 0 and base not in seen:
            out.append(base)
            seen[base] = 1
            continue
        # bump until unique
        while True:
            i += 1
            cand = f"{base}_{i}"
            if cand not in seen:
                out.append(cand)
                seen[base] = i
                seen[cand] = 1
                break
    return out

def detect_csv_delimiter(sample_bytes: bytes) -> str:
    try:
        sample = sample_bytes.decode("utf-8", errors="ignore")
        dialect = csv.Sniffer().sniff(sample[:4096], delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter
    except Exception:
        return ","

def bytes_to_text(b: bytes) -> str:
    try:
        return b.decode("utf-8-sig")
    except UnicodeDecodeError:
        return b.decode("latin-1", errors="replace")

@dataclass
class FileCheck:
    original_name: str
    df: pd.DataFrame
    issues: list[str]
    acceptable: bool
    sanitized_table: str
    sanitized_columns: list[str]
    row_count: int
    col_count: int
    csv_bytes: bytes
    s3_key: str | None = None

# ──────────────────────────────────────────────────────────────────────────────
# Validation / Normalization
# ──────────────────────────────────────────────────────────────────────────────

def validate_and_normalize(upload) -> FileCheck:
    """
    - Accept CSV or XLSX
    - Parse all cells as TEXT (preserve leading zeros, etc.)
    - Structural checks: rows > 0, columns > 0, non-blank headers, no ragged rows
    - Sanitize target table + column names
    - Normalize to a UTF-8 CSV (comma delimiter) with header row preserved
    """
    name = upload.name
    lower = name.lower()
    issues: list[str] = []

    # Read to DataFrame (strings)
    try:
        if lower.endswith(".csv"):
            raw = upload.getvalue()
            delim = detect_csv_delimiter(raw)
            txt = bytes_to_text(raw)
            df = pd.read_csv(
                io.StringIO(txt),
                sep=delim,
                dtype=str,
                engine="python",
                on_bad_lines="error",
                keep_default_na=False,
            )
        elif lower.endswith(".xlsx"):
            raw = upload.getvalue()
            df = pd.read_excel(
                io.BytesIO(raw),
                dtype=str,
                engine="openpyxl",
            ).astype(str).fillna("")
        else:
            issues.append("Unsupported file type. Only CSV and XLSX are allowed.")
            df = pd.DataFrame()
    except Exception as e:
        issues.append(f"Failed to parse file: {e}")
        df = pd.DataFrame()

    # Structural checks
    if df.shape[0] == 0:
        issues.append("No data rows found.")
    if df.shape[1] == 0:
        issues.append("No columns detected (header row missing or empty).")

    # Header checks
    orig_cols = [str(c) for c in df.columns.tolist()]
    if any(c.strip() == "" for c in orig_cols):
        issues.append("One or more column headers are blank.")
    if len(set(orig_cols)) != len(orig_cols):
        issues.append("Duplicate column headers detected; they will be auto-uniquified.")

    # Sanitize Snowflake identifiers
    table_stem = name.rsplit(".", 1)[0]
    sanitized_table = sanitize_identifier(table_stem, default="REPORT")
    sanitized_cols = ensure_unique([sanitize_identifier(c, default="COL") for c in orig_cols])

    # Normalize to UTF-8 CSV (comma delimiter)
    try:
        out = io.StringIO()
        df.to_csv(out, index=False)  # comma delimiter by default
        csv_bytes = out.getvalue().encode("utf-8")
    except Exception as e:
        issues.append(f"Failed to normalize to CSV: {e}")
        csv_bytes = b""

    acceptable = len(issues) == 0

    return FileCheck(
        original_name=name,
        df=df,
        issues=issues,
        acceptable=acceptable,
        sanitized_table=sanitized_table,
        sanitized_columns=sanitized_cols,
        row_count=int(df.shape[0]),
        col_count=int(df.shape[1]),
        csv_bytes=csv_bytes,
    )

# ──────────────────────────────────────────────────────────────────────────────
# S3 / Snowflake
# ──────────────────────────────────────────────────────────────────────────────

def s3_client():
    session = boto3.Session(
        aws_access_key_id=st.secrets["aws"]["access_key_id"],
        aws_secret_access_key=st.secrets["aws"]["secret_access_key"],
        region_name=CFG.aws_region,
    )
    return session.client("s3")

def sf_connect():
    return snowflake.connector.connect(
        account=CFG.sf_account,
        user=CFG.sf_user,
        password=CFG.sf_password,
        role=CFG.sf_role,
        warehouse=CFG.sf_wh,
        database=CFG.sf_db,
        schema=CFG.sf_schema,
    )

def new_session_prefix() -> str:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return f"uploads/{ts}_{uuid.uuid4().hex[:8]}"

def s3_key_name(session_prefix: str, original_name: str) -> str:
    stem = original_name.rsplit(".", 1)[0]
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", stem)
    return f"{CFG.s3_prefix}/{session_prefix}/{safe}.csv"  # stored as CSV always

def upload_bytes_to_s3(cli, key: str, data: bytes):
    cli.upload_fileobj(io.BytesIO(data), CFG.s3_bucket, key)

def list_keys_under(cli, prefix: str) -> list[str]:
    keys: list[str] = []
    token = None
    while True:
        params = dict(Bucket=CFG.s3_bucket, Prefix=prefix)
        if token:
            params["ContinuationToken"] = token
        resp = cli.list_objects_v2(**params)
        for c in resp.get("Contents", []):
            keys.append(c["Key"])
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys

def create_table_if_missing(cur, full_table: str, cols: list[str]):
    cols_sql = ", ".join(f"{c} VARCHAR" for c in cols)
    cur.execute(f"CREATE TABLE IF NOT EXISTS {full_table} ({cols_sql});")

def table_count(cur, full_table: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {full_table}")
    return int(cur.fetchone()[0])

def copy_one_file(
    cur,
    full_table: str,
    stage: str,
    stage_dir: str,  # relative to stage root
    file_basename: str,
):
    """
    COPY INTO <table>
    FROM @<stage>/<stage_dir>
    FILES = ( '<basename>.csv' )
    FILE_FORMAT: CSV, SKIP_HEADER=1, delimiter=',', optionally enclosed by '"',
    TRIM_SPACE=TRUE, EMPTY_FIELD_AS_NULL=FALSE
    """
    sql = f"""
        COPY INTO {full_table}
        FROM @{stage}/{stage_dir}
        FILES = ('{file_basename}')
        FILE_FORMAT = (
            TYPE=CSV
            SKIP_HEADER=1
            FIELD_DELIMITER=','
            FIELD_OPTIONALLY_ENCLOSED_BY='\"'
            EMPTY_FIELD_AS_NULL=FALSE
            TRIM_SPACE=TRUE
        )
        ON_ERROR='ABORT_STATEMENT';
    """
    cur.execute(sql)
    return cur.fetchall()  # copy result rows

# ──────────────────────────────────────────────────────────────────────────────
# UI — Steps 1..4
# ──────────────────────────────────────────────────────────────────────────────

if "session_prefix" not in st.session_state:
    st.session_state.session_prefix = ""
if "file_checks" not in st.session_state:
    st.session_state.file_checks: list[FileCheck] = []
if "selected" not in st.session_state:
    st.session_state.selected: dict[str, bool] = {}

st.markdown("### 1) Select distributor reports")
uploads = st.file_uploader(
    "Upload CSV or Excel (.xlsx) — multiple allowed",
    type=["csv", "xlsx"],
    accept_multiple_files=True,
)

if uploads:
    checks: list[FileCheck] = [validate_and_normalize(u) for u in uploads]
    st.session_state.file_checks = checks

    st.markdown("### 2) Initial discrepancy checks")
    for idx, chk in enumerate(checks, start=1):
        st.subheader(f"{idx}. {chk.original_name}")
        c1, c2 = st.columns([3, 2])
        with c1:
            st.write(f"**Rows**: {chk.row_count} • **Columns**: {chk.col_count}")
            if chk.issues:
                st.error("Issues detected:\n- " + "\n- ".join(chk.issues))
            else:
                st.success("No discrepancies found.")
            st.caption("Preview (first 10 rows)")
            st.dataframe(chk.df.head(10), use_container_width=True)
        with c2:
            st.write("**Snowflake target**")
            st.write(f"Table: `{CFG.sf_db}.{CFG.sf_schema}.{chk.sanitized_table}`")
            st.write("Columns:")
            st.code(", ".join(chk.sanitized_columns), language="text")
            key = f"sel_{idx}"
            st.session_state.selected[key] = st.checkbox(
                "Ready for ingestion",
                value=chk.acceptable,
                disabled=not chk.acceptable,
                help="Enabled only if file passed validation."
            )
    st.divider()

    st.markdown("### 3) Submit")
    if st.button("Copy selected files to S3 and Ingest into Snowflake", type="primary"):
        chosen = [
            chk for i, chk in enumerate(st.session_state.file_checks, start=1)
            if st.session_state.selected.get(f"sel_{i}", False)
        ]
        if not chosen:
            st.warning("No files selected.")
        else:
            if not st.session_state.session_prefix:
                st.session_state.session_prefix = new_session_prefix()

            s3 = s3_client()
            uploaded_keys: list[str] = []

            st.info("Uploading to S3…")
            prog = st.progress(0.0)
            for n, chk in enumerate(chosen, start=1):
                try:
                    key = s3_key_name(st.session_state.session_prefix, chk.original_name)
                    upload_bytes_to_s3(s3, key, chk.csv_bytes)
                    chk.s3_key = key
                    uploaded_keys.append(key)
                    st.write(f"Uploaded: `s3://{CFG.s3_bucket}/{key}`")
                except botocore.exceptions.BotoCoreError as e:
                    st.error(f"S3 upload failed for {chk.original_name}: {e}")
                    chk.s3_key = None
                prog.progress(n / len(chosen))

            # Identify files in S3 under this session
            session_prefix_full = f"{CFG.s3_prefix}/{st.session_state.session_prefix}"
            keys_found = list_keys_under(s3, session_prefix_full)
            st.success(f"Identified {len(keys_found)} file(s) in s3://{CFG.s3_bucket}/{session_prefix_full}")

            st.markdown("### 4) Ingestion Results (Snowflake)")
            try:
                with sf_connect() as con:
                    cur = con.cursor()
                    try:
                        cur.execute(f"USE WAREHOUSE {CFG.sf_wh}")
                        cur.execute(f"USE DATABASE {CFG.sf_db}")
                        cur.execute(f"USE SCHEMA {CFG.sf_schema}")
                        if CFG.sf_role:
                            cur.execute(f"USE ROLE {CFG.sf_role}")

                        # Stage-relative directory (exclude <prefix> because stage URL already points to it)
                        stage_dir = st.session_state.session_prefix  # e.g., uploads/20240901_xxxx

                        for chk in chosen:
                            if not chk.s3_key:
                                st.error(f"Skipping {chk.original_name}: not uploaded.")
                                continue

                            full_table = f"{CFG.sf_db}.{CFG.sf_schema}.{chk.sanitized_table}"
                            st.subheader(f"Ingest: {chk.original_name} → `{full_table}`")

                            # Ensure table exists (all VARCHAR, order preserved)
                            create_table_if_missing(cur, full_table, chk.sanitized_columns)
                            st.caption("Ensured target table exists.")

                            # Count before copy to validate per-file row delta
                            before = table_count(cur, full_table)

                            # Basename relative to stage_dir
                            basename = os.path.basename(chk.s3_key)
                            # s3_key is "<prefix>/<session_dir>/<basename>", so:
                            # we want just "<basename>" for FILES=() under FROM @stage/<session_dir>
                            basename = basename

                            copy_result = copy_one_file(
                                cur=cur,
                                full_table=full_table,
                                stage=CFG.sf_stage,
                                stage_dir=stage_dir,
                                file_basename=basename,
                            )
                            con.commit()

                            after = table_count(cur, full_table)
                            loaded = after - before
                            src = chk.row_count

                            if loaded == src:
                                st.success(f"Copied {loaded} rows. ✅ Counts match (source={src}, loaded={loaded}).")
                            else:
                                st.warning(f"Row delta mismatch (source={src}, loaded={loaded}). Review COPY results below.")

                            with st.expander("Snowflake COPY result rows"):
                                st.table(pd.DataFrame(copy_result, columns=["result"]))
                    finally:
                        cur.close()
                st.success("Process complete.")
            except Exception as e:
                st.error(f"Snowflake ingestion failed: {e}")

else:
    st.info("Upload CSV or Excel (.xlsx) files to begin.")


