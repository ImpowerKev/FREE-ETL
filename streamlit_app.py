from __future__ import annotations

import io
import re
import csv
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

# ────────────────────────────────────────────────────────────────────────────────────────────────────
# Page + global safety switch
# ────────────────────────────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Compliance Intake (S3‑only)", page_icon="✅", layout="wide")
st.write(" ")  # ensures something renders even if later blocks stop
st.title("✅ Distributor Reports — Compliance Intake (S3‑only)")

# NEVER import boto3 at module import; do it lazily inside functions only.
# This prevents crashes when requirements or secrets aren't ready.

# ────────────────────────────────────────────────────────────────────────────────────────────────────
# Config loader — tolerant, never throws
# ────────────────────────────────────────────────────────────────────────────────────────────────────
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
    return AppConfig(
        aws_region=aws.get("region", "us-east-1"),
        s3_bucket=aws.get("bucket", ""),
        s3_prefix=(aws.get("prefix", "ingestion") or "ingestion").strip("/"),
        aws_ready=len(missing) == 0,
        missing_keys=missing,
        max_file_mb=int(app_opts.get("max_file_mb", 50)),
        allow_xlsx=bool(app_opts.get("allow_xlsx", True)),
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
        "S3 secrets not configured — running in **Offline Mode**. "
        "Uploads are disabled. You can still validate files and download a normalized ZIP."
    )
    if CFG.missing_keys:
        st.info("Missing secrets → `aws.` keys: " + ", ".join(CFG.missing_keys))

# ────────────────────────────────────────────────────────────────────────────────────────────────────
# Utilities (safe, no AWS touched here)
# ────────────────────────────────────────────────────────────────────────────────────────────────────
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

def new_session_prefix() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"uploads/{ts}_{uuid.uuid4().hex[:8]}"

def s3_client():
    # Lazy import to avoid hard failure if boto3 not ready
    try:
        import boto3  # type: ignore
    except Exception as e:
        raise RuntimeError("boto3 not installed or failed to import") from e

    try:
        aws = st.secrets["aws"]
        session = boto3.Session(
            aws_access_key_id=aws["access_key_id"],
            aws_secret_access_key=aws["secret_access_key"],
            region_name=CFG.aws_region,
        )
        return session.client("s3")
    except Exception as e:
        raise RuntimeError("AWS secrets invalid or missing. Configure [aws] block.") from e

def s3_key_for(session_prefix: str, original_name: str) -> str:
    stem = original_name.rsplit(".", 1)[0]
    safe_stem = SAFE_CHAR_RE.sub("_", stem)
    return f"{CFG.s3_prefix}/{session_prefix}/{safe_stem}.csv"

def explain_boto_error(e: Exception) -> str:
    # Defensive messages without importing botocore at module import
    msg = str(e)
    if "AccessDenied" in msg:
        return "Access denied for the provided IAM credentials on this bucket/prefix."
    if "NoSuchBucket" in msg:
        return "Bucket not found. Check `[aws].bucket`."
    if "EndpointConnectionError" in msg:
        return "Network/endpoint error reaching S3. Check region or network."
    if "InvalidAccessKeyId" in msg or "SignatureDoesNotMatch" in msg:
        return "Invalid AWS keys. Verify `access_key_id` / `secret_access_key`."
    return f"S3 error: {msg}"

# ────────────────────────────────────────────────────────────────────────────────────────────────────
# Validation / Normalization (no AWS here)
# ────────────────────────────────────────────────────────────────────────────────────────────────────
@dataclass
class FileCheck:
    original_name: str
    issues: list[str]
    acceptable: bool
    df_head: pd.DataFrame
    row_count: int
    col_count: int
    csv_bytes: bytes

def validate_and_normalize(upload) -> FileCheck:
    name_lower = upload.name.lower()
    issues: list[str] = []

    # size (robust even if .size missing)
    try:
        size_bytes = getattr(upload, "size", None) or len(upload.getvalue())
    except Exception:
        size_bytes = 0
    size_mb = size_bytes / (1024 * 1024)
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
                keep_default_na=False,
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

    # Normalize to UTF‑ CSV
    csv_bytes = b""
    if len(issues) == 0 and not df.empty:
        try:
            buf = io.StringIO()
            df.to_csv(buf, index=False)
            csv_bytes = buf.getvalue().encode("utf-8")
        except Exception as e:
            issues.append(f"Failed to normalize to CSV: {e}")

    return FileCheck(
        original_name=upload.name,
        issues=issues,
        acceptable=(len(issues) == 0),
        df_head=df.head(10) if not df.empty else pd.DataFrame(),
        row_count=int(df.shape[0]) if not df.empty else 0,
        col_count=int(df.shape[1]) if not df.empty else 0,
        csv_bytes=csv_bytes,
    )

# ────────────────────────────────────────────────────────────────────────────────────────────────────
# MAIN UI — fully guarded, no AWS calls unless ONLINE and user clicks
# ────────────────────────────────────────────────────────────────────────────────────────────────────

def main():
    # Connection test block (never errors out)
    st.subheader("Connection")
    left, right = st.columns(2)
    with left:
        test_btn = st.button("Test S3 connection", disabled=not CFG.aws_ready)
        if test_btn:
            try:
                client = s3_client()
                # minimal call that doesn’t require special perms beyond list
                client.list_objects_v2(Bucket=CFG.s3_bucket, Prefix=CFG.s3_prefix, MaxKeys=1)
                st.success("✅ S3 connection OK and prefix is reachable.")
            except Exception as e:
                st.error(explain_boto_error(e))
    with right:
        st.caption("If disabled, add `[aws]` secrets to enable.")

    st.divider()

    # Step 1: Upload
    st.markdown("### 1) Select distributor reports")
    uploads = st.file_uploader(
        "Upload CSV or Excel (.xlsx) — multiple allowed",
        type=["csv", "xlsx"] if CFG.allow_xlsx else ["csv"],
        accept_multiple_files=True,
    )

    # Step 2: Checks
    if uploads:
        checks = [validate_and_normalize(u) for u in uploads]
        st.markdown("### 2) Initial discrepancy checks")
        chosen = []
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
                    pick = st.checkbox(
                        "Ready for ingestion (S3 upload)",
                        value=chk.acceptable,
                        disabled=not chk.acceptable,
                        key=f"sel_{i}",
                    )
                    if pick:
                        chosen.append(chk)
        st.divider()

        # Step 3: Actions
        st.markdown("### 3) Submit")
        a, b, _ = st.columns([1, 1, 3])
        with a:
            do_upload = st.button("Copy selected files to S3", type="primary", disabled=not CFG.aws_ready)
        with b:
            make_zip = st.button("Download normalized ZIP (offline)")

        # ONLINE upload
        if do_upload:
            if not chosen:
                st.warning("No files selected.")
            else:
                session_prefix = new_session_prefix()
                try:
                    client = s3_client()
                except Exception as e:
                    st.error(explain_boto_error(e))
                    st.stop()

                with st.status("Uploading to S3…", expanded=True) as status:
                    uploaded = []
                    for n, chk in enumerate(chosen, start=1):
                        key = s3_key_for(session_prefix, chk.original_name)
                        try:
                            # lazy import to keep module import safe
                            from botocore.exceptions import BotoCoreError, ClientError  # type: ignore
                        except Exception:
                            BotoCoreError = ClientError = Exception  # noqa: N806
                        try:
                            client.upload_fileobj(io.BytesIO(chk.csv_bytes), CFG.s3_bucket, key)
                            st.write(f"Uploaded {n}/{len(chosen)}: `s3://{CFG.s3_bucket}/{key}`")
                            uploaded.append({
                                "original_name": chk.original_name,
                                "s3_uri": f"s3://{CFG.s3_bucket}/{key}",
                                "rows": chk.row_count,
                                "cols": chk.col_count,
                                "uploaded_at_utc": datetime.now(timezone.utc).isoformat(),
                            })
                        except (BotoCoreError, ClientError, Exception) as e:  # catch-all to avoid restart loops
                            st.error(f"{chk.original_name} → {explain_boto_error(e)}")

                    # Write manifest (best‑effort)
                    manifest = {
                        "session": session_prefix,
                        "bucket": CFG.s3_bucket,
                        "prefix": CFG.s3_prefix,
                        "uploaded_files": uploaded,
                    }
                    try:
                        client.put_object(
                            Bucket=CFG.s3_bucket,
                            Key=f"{CFG.s3_prefix}/{session_prefix}/manifest.json",
                            Body=json.dumps(manifest, indent=2).encode("utf-8"),
                            ContentType="application/json",
                        )
                        st.success("Manifest written.")
                    except Exception as e:
                        st.warning("Manifest write failed: " + explain_boto_error(e))
                    status.update(label="Upload complete", state="complete")

        # OFFLINE bundle — always available
        if make_zip:
            if not chosen:
                st.warning("No files selected.")
            else:
                import zipfile
                zip_buf = io.BytesIO()
                with zipfile.ZipFile(zip_buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                    meta = []
                    for chk in chosen:
                        fname = SAFE_CHAR_RE.sub("_", chk.original_name.rsplit(".", 1)[0]) + ".csv"
                        zf.writestr(fname, chk.csv_bytes)
                        meta.append({"original_name": chk.original_name, "normalized_name": fname,
                                     "rows": chk.row_count, "cols": chk.col_count})
                    zf.writestr("manifest.json", json.dumps({"generated_at_utc": datetime.now(timezone.utc).isoformat(),
                                                             "files": meta}, indent=2))
                zip_buf.seek(0)
                st.download_button("Download bundle.zip", zip_buf, file_name="bundle.zip", mime="application/zip")

# Run main with a global catch so the UI always shows errors instead of restarting
try:
    main()
except Exception as e:
    st.error("Unhandled error. The app stayed up so you can see it. Details below.")
    st.exception(e)
