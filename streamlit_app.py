from __future__ import annotations

import io
import re
import csv
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
import typing as t

import pandas as pd
import streamlit as st

# Third‑party SDKs
import boto3
from botocore.exceptions import (
    ClientError,
    NoCredentialsError,
    PartialCredentialsError,
    EndpointConnectionError,
    ParamValidationError,
    BotoCoreError,
)

# ──────────────────────────────────────────────────────────────────────────
# Page
# ──────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Compliance Intake (S3‑only)", page_icon="✅", layout="wide")
st.title("✅ Distributor Reports — Compliance Intake (S3‑only)")

# ──────────────────────────────────────────────────────────────────────────
# Config via st.secrets — tolerant loader (never crashes if empty)
# ──────────────────────────────────────────────────────────────────────────

@dataclass
class AppConfig:
    aws_region: str
    s3_bucket: str
    s3_prefix: str
    aws_ready: bool
    missing_keys: list[str]
    max_file_mb: int
    allow_xlsx: bool

def load_cfg() -> AppConfig:
    aws = st.secrets.get("aws", {}) or {}
    app_opts = st.secrets.get("app", {}) or {}

    required = ["access_key_id", "secret_access_key", "region", "bucket", "prefix"]
    missing = [k for k in required if not aws.get(k)]
    aws_ready = len(missing) == 0

    return AppConfig(
        aws_region = aws.get("region", "us-east-1"),
        s3_bucket  = aws.get("bucket", ""),
        s3_prefix  = (aws.get("prefix", "ingestion") or "ingestion").strip("/"),
        aws_ready  = aws_ready,
        missing_keys = missing,
        max_file_mb = int(app_opts.get("max_file_mb", 50)),
        allow_xlsx  = bool(app_opts.get("allow_xlsx", True)),
    )

CFG = load_cfg()

with st.expander("Runtime (non‑sensitive)", expanded=False):
    st.code(
        f"""Mode:          {"ONLINE (S3 enabled)" if CFG.aws_ready else "OFFLINE (S3 disabled)"}
AWS Region:   {CFG.aws_region}
S3 Bucket:    {CFG.s3_bucket or "(not set)"}
S3 Prefix:    {CFG.s3_prefix}
Max File (MB):{CFG.max_file_mb}
Excel allowed:{CFG.allow_xlsx}""",
        language="text",
    )

if not CFG.aws_ready:
    st.warning(
        "S3 is not configured yet — running in **Offline Mode**. "
        "Uploads are disabled. You can still validate files and download a normalized bundle."
    )
    if CFG.missing_keys:
        st.info("Missing secrets → `aws.`: " + ", ".join(CFG.missing_keys))

# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────

SAFE_CHAR_RE = re.compile(r"[^A-Za-z0-9._-]")

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

def s3_client():
    # Build client only when needed; surface clear errors in UI.
    try:
        session = boto3.Session(
            aws_access_key_id=st.secrets["aws"]["access_key_id"],
            aws_secret_access_key=st.secrets["aws"]["secret_access_key"],
            region_name=CFG.aws_region,
        )
        return session.client("s3")
    except KeyError:
        raise NoCredentialsError()

def new_session_prefix() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"uploads/{ts}_{uuid.uuid4().hex[:8]}"

def s3_key_for(session_prefix: str, original_name: str) -> str:
    stem = original_name.rsplit(".", 1)[0]
    safe_stem = SAFE_CHAR_RE.sub("_", stem)
    return f"{CFG.s3_prefix}/{session_prefix}/{safe_stem}.csv"

def upload_bytes_to_s3(cli, key: str, data: bytes):
    cli.upload_fileobj(io.BytesIO(data), CFG.s3_bucket, key)

def list_s3_keys(cli, prefix: str) -> list[str]:
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

def explain_boto_error(e: Exception) -> str:
    if isinstance(e, NoCredentialsError):
        return "No AWS credentials found. Add `aws.access_key_id` and `aws.secret_access_key` to secrets."
    if isinstance(e, PartialCredentialsError):
        return "Incomplete AWS credentials. One or more keys are missing."
    if isinstance(e, EndpointConnectionError):
        return "Network/endpoint error reaching S3. Check region or network."
    if isinstance(e, ParamValidationError):
        return "Invalid parameter sent to S3 (bucket/prefix/name)."
    if isinstance(e, ClientError):
        code = e.response.get("Error", {}).get("Code", "Unknown")
        msg  = e.response.get("Error", {}).get("Message", str(e))
        if code == "AccessDenied":
            return "Access denied for the provided IAM credentials on this bucket/prefix."
        return f"S3 client error ({code}): {msg}"
    if isinstance(e, BotoCoreError):
        return f"Low‑level AWS SDK error: {e}"
    return f"Unexpected error: {e}"

# ──────────────────────────────────────────────────────────────────────────
# Validation / Normalization
# ──────────────────────────────────────────────────────────────────────────

@dataclass
class FileCheck:
    original_name: str
    issues: list[str]
    acceptable: bool
    df_head: pd.DataFrame
    row_count: int
    col_count: int
    csv_bytes: bytes
    s3_key: str | None = None

def validate_and_normalize(upload) -> FileCheck:
    """Read CSV/XLSX as text, run structural checks, normalize to UTF‑8 CSV."""
    name_lower = upload.name.lower()
    issues: list[str] = []

    # size guard (works even if .size is missing in some Streamlit versions)
    size_bytes = getattr(upload, "size", None)
    if size_bytes is None:
        try:
            size_bytes = len(upload.getvalue())
        except Exception:
            size_bytes = 0
    size_mb = size_bytes / (1024 * 1024) if size_bytes else 0.0
    if size_mb > CFG.max_file_mb:
        issues.append(f"File exceeds max size ({size_mb:.1f} MB > {CFG.max_file_mb} MB).")

    df = pd.DataFrame()
    try:
        if name_lower.endswith(".csv"):
            raw = upload.getvalue()
            delim = detect_csv_delimiter(raw)
            txt = bytes_to_text(raw)
            df = pd.read_csv(
                io.StringIO(txt),
                dtype=str,
                sep=delim,
                engine="python",
                on_bad_lines="error",
                keep_default_na=False
            )
        elif name_lower.endswith(".xlsx") and CFG.allow_xlsx:
            raw = upload.getvalue()
            df = pd.read_excel(io.BytesIO(raw), dtype=str, engine="openpyxl").astype(str).fillna("")
        else:
            issues.append("Unsupported file type. Use CSV or XLSX.")
    except Exception as e:
        issues.append(f"Failed to parse file: {e}")

    if not df.empty:
        if df.shape[0] == 0:
            issues.append("No data rows found.")
        if df.shape[1] == 0:
            issues.append("No columns detected (empty header).")
        headers = [str(c) for c in df.columns.tolist()]
        if any(c.strip() == "" for c in headers):
            issues.append("One or more column headers are blank.")
        if len(set(headers)) != len(headers):
            issues.append("Duplicate column headers detected.")

    # Normalize to UTF‑8 CSV with comma delimiter
    csv_bytes = b""
    if len(issues) == 0 and not df.empty:
        try:
            buf = io.StringIO()
            df.to_csv(buf, index=False)
            csv_bytes = buf.getvalue().encode("utf-8")
        except Exception as e:
            issues.append(f"Failed to normalize to CSV: {e}")

    acceptable = len(issues) == 0
    return FileCheck(
        original_name=upload.name,
        issues=issues,
        acceptable=acceptable,
        df_head=df.head(10) if not df.empty else pd.DataFrame(),
        row_count=int(df.shape[0]) if not df.empty else 0,
        col_count=int(df.shape[1]) if not df.empty else 0,
        csv_bytes=csv_bytes,
    )

# ──────────────────────────────────────────────────────────────────────────
# UI state
# ──────────────────────────────────────────────────────────────────────────
if "session_prefix" not in st.session_state:
    st.session_state.session_prefix = ""
if "file_checks" not in st.session_state:
    st.session_state.file_checks: list[FileCheck] = []
if "selected" not in st.session_state:
    st.session_state.selected: dict[str, bool] = {}

# ──────────────────────────────────────────────────────────────────────────
# Connectivity test (only enabled when secrets exist)
# ──────────────────────────────────────────────────────────────────────────
st.subheader("Connection")
cols = st.columns(2)
with cols[0]:
    if st.button("Test S3 connection", disabled=not CFG.aws_ready):
        try:
            s3 = s3_client()
            # quick validation: head bucket OR list prefix
            s3.list_objects_v2(Bucket=CFG.s3_bucket, Prefix=CFG.s3_prefix, MaxKeys=1)
            st.success("✅ S3 connection OK and prefix is reachable.")
        except Exception as e:
            st.error(explain_boto_error(e))

with cols[1]:
    st.caption("Tip: if disabled, add `[aws]` secrets to enable.")

st.divider()

# ──────────────────────────────────────────────────────────────────────────
# Step 1: Upload & Step 2: Checks
# ──────────────────────────────────────────────────────────────────────────
st.markdown("### 1) Select distributor reports")
uploads = st.file_uploader(
    "Upload CSV or Excel (.xlsx) — multiple allowed",
    type=["csv", "xlsx"] if CFG.allow_xlsx else ["csv"],
    accept_multiple_files=True,
)

if uploads:
    checks = [validate_and_normalize(u) for u in uploads]
    st.session_state.file_checks = checks

    st.markdown("### 2) Initial discrepancy checks")
    for i, chk in enumerate(checks, start=1):
        with st.container(border=True):
            st.subheader(f"{i}. {chk.original_name}")
            c1, c2 = st.columns([3, 2], vertical_alignment="top")
            with c1:
                st.write(f"**Rows**: {chk.row_count} • **Columns**: {chk.col_count}")
                if chk.issues:
                    st.error("Issues detected:\n- " + "\n- ".join(chk.issues))
                else:
                    st.success("No discrepancies found.")
                if chk.row_count > 0:
                    st.caption("Preview (first 10 rows)")
                    st.dataframe(chk.df_head, use_container_width=True)
            with c2:
                key = f"sel_{i}"
                st.session_state.selected[key] = st.checkbox(
                    "Ready for ingestion (S3 upload)",
                    value=chk.acceptable,
                    disabled=not chk.acceptable,
                    help="Enabled only if the file passed validation."
                )

    st.divider()

    # ──────────────────────────────────────────────────────────────────────────
    # Step 3A: S3 Submit (ONLINE)
    # ──────────────────────────────────────────────────────────────────────────
    st.markdown("### 3) Submit")
    btn_cols = st.columns([1, 1, 3])
    with btn_cols[0]:
        do_upload = st.button("Copy selected files to S3", type="primary", disabled=not CFG.aws_ready)

    # ──────────────────────────────────────────────────────────────────────────
    # Step 3B: Offline bundle (always available)
    # ──────────────────────────────────────────────────────────────────────────
    with btn_cols[1]:
        make_bundle = st.button("Download normalized ZIP (offline)")

    chosen = [
        chk for idx, chk in enumerate(st.session_state.file_checks, start=1)
        if st.session_state.selected.get(f"sel_{idx}", False)
    ]

    if do_upload:
        if not chosen:
            st.warning("No files selected.")
        else:
            if not st.session_state.session_prefix:
                st.session_state.session_prefix = new_session_prefix()

            try:
                s3 = s3_client()
            except Exception as e:
                st.error(explain_boto_error(e))
                st.stop()

            with st.status("Uploading to S3…", expanded=True) as status:
                uploaded = []
                total = len(chosen)
                for n, chk in enumerate(chosen, start=1):
                    key = s3_key_for(st.session_state.session_prefix, chk.original_name)
                    try:
                        upload_bytes_to_s3(s3, key, chk.csv_bytes)
                        chk.s3_key = key
                        uploaded.append({
                            "original_name": chk.original_name,
                            "s3_uri": f"s3://{CFG.s3_bucket}/{key}",
                            "rows": chk.row_count,
                            "cols": chk.col_count,
                            "uploaded_at_utc": datetime.now(timezone.utc).isoformat()
                        })
                        st.write(f"Uploaded {n}/{total}: `s3://{CFG.s3_bucket}/{key}`")
                    except Exception as e:
                        st.error(f"{chk.original_name} → {explain_boto_error(e)}")
                        chk.s3_key = None

                # Try to list to confirm visibility (optional)
                try:
                    session_prefix_full = f"{CFG.s3_prefix}/{st.session_state.session_prefix}"
                    keys_found = list_s3_keys(s3, session_prefix_full)
                    st.write(f"Identified {len(keys_found)} file(s) in `s3://{CFG.s3_bucket}/{session_prefix_full}`")
                except Exception as e:
                    st.warning("Post-upload list failed: " + explain_boto_error(e))

                # Write manifest
                manifest = {
                    "session": st.session_state.session_prefix,
                    "bucket": CFG.s3_bucket,
                    "prefix": CFG.s3_prefix,
                    "uploaded_files": uploaded
                }
                manifest_key = f"{CFG.s3_prefix}/{st.session_state.session_prefix}/manifest.json"
                try:
                    upload_bytes_to_s3(s3, manifest_key, json.dumps(manifest, indent=2).encode("utf-8"))
                    st.success(f"Manifest written: `s3://{CFG.s3_bucket}/{manifest_key}`")
                except Exception as e:
                    st.warning("Manifest write failed: " + explain_boto_error(e))

                status.update(label="Upload step finished", state="complete")

    # Offline ZIP bundle
    if make_bundle:
        if not chosen:
            st.warning("No files selected.")
        else:
            import zipfile
            zip_buf = io.BytesIO()
            with zipfile.ZipFile(zip_buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                meta = []
                for chk in chosen:
                    filename = SAFE_CHAR_RE.sub("_", chk.original_name.rsplit(".", 1)[0]) + ".csv"
                    zf.writestr(filename, chk.csv_bytes)
                    meta.append({
                        "original_name": chk.original_name,
                        "normalized_name": filename,
                        "rows": chk.row_count,
                        "cols": chk.col_count,
                    })
                manifest = {
                    "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                    "files": meta
                }
                zf.writestr("manifest.json", json.dumps(manifest, indent=2))
            zip_buf.seek(0)
            st.download_button(
                label="Download bundle.zip",
                data=zip_buf,
                file_name="bundle.zip",
                mime="application/zip"
            )

else:
    st.info("Upload CSV or Excel (.xlsx) files to begin.")
