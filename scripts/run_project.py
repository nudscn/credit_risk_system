#!/usr/bin/env python3
"""MVP pipeline runner with extraction and rule execution.

Pipeline:
1) ingest manifest
2) extract text and subject-value candidates from PDFs
3) quality issue generation
4) mapping coverage summary
5) variance placeholder (cross-source ready)
6) reconciliation execution
7) analysis execution (basic)
8) risk report generation

Outputs are written to /outputs and events are logged to /logs/pipeline_events.jsonl.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def dump_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def file_md5(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def log_event(log_file: Path, stage: str, level: str, message: str, payload: Dict[str, Any]) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    event = {
        "ts": utc_now(),
        "stage": stage,
        "level": level,
        "message": message,
        "payload": payload,
    }
    with log_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")


def parse_year_from_name(name: str) -> Optional[str]:
    m = re.search(r"(20\d{2})", name)
    return m.group(1) if m else None


def normalize_number(token: str) -> Optional[float]:
    cleaned = token.strip().replace(",", "")
    if not cleaned:
        return None
    negative = False
    if cleaned.startswith("(") and cleaned.endswith(")"):
        negative = True
        cleaned = cleaned[1:-1]
    if cleaned.startswith("-"):
        negative = True
        cleaned = cleaned[1:]
    try:
        value = float(cleaned)
        return -value if negative else value
    except ValueError:
        return None


def normalize_for_match(text: str) -> str:
    return re.sub(r"[\\s,:;，。\\(\\)（）\\[\\]【】\\-_/]+", "", text or "").lower()


def extract_pdf_pages(pdf_path: Path) -> Dict[str, Any]:
    pages: List[Dict[str, Any]] = []
    engine = None
    ocr_status = "not_attempted"
    ocr_error: Optional[str] = None

    try:
        from pypdf import PdfReader  # type: ignore

        engine = "pypdf"
        reader = PdfReader(str(pdf_path))
        for idx, page in enumerate(reader.pages, start=1):
            text = page.extract_text() or ""
            pages.append({"page": idx, "text": text, "chars": len(text)})
    except Exception:
        try:
            import PyPDF2  # type: ignore

            engine = "PyPDF2"
            with pdf_path.open("rb") as f:
                reader = PyPDF2.PdfReader(f)
                for idx, page in enumerate(reader.pages, start=1):
                    text = page.extract_text() or ""
                    pages.append({"page": idx, "text": text, "chars": len(text)})
        except Exception:
            # Local fallback: use Swift PDFKit extractor script.
            swift_script = Path(__file__).resolve().parent / "pdf_extract.swift"
            if swift_script.exists():
                try:
                    proc = subprocess.run(
                        ["swift", str(swift_script), str(pdf_path)],
                        check=False,
                        capture_output=True,
                        text=True,
                        timeout=180,
                    )
                    if proc.returncode == 0 and proc.stdout.strip():
                        parsed = json.loads(proc.stdout)
                        if isinstance(parsed, dict) and parsed.get("status") == "ok":
                            pages = parsed.get("pages", [])
                            engine = parsed.get("engine", "swift_pdfkit")
                        else:
                            return {
                                "engine": None,
                                "status": "failed",
                                "pages": [],
                                "error": "Swift PDFKit extraction returned invalid JSON",
                            }
                    else:
                        return {
                            "engine": None,
                            "status": "failed",
                            "pages": [],
                            "error": f"Swift PDFKit extraction failed: {proc.stderr.strip() or proc.stdout.strip()}",
                        }
                except Exception as e:  # noqa: F841
                    return {
                        "engine": None,
                        "status": "failed",
                        "pages": [],
                        "error": "Swift PDFKit extraction failed due to runtime error",
                    }

            if engine is None:
                return {
                    "engine": None,
                    "status": "failed",
                    "pages": [],
                    "error": "No PDF extraction engine available (pypdf/PyPDF2/swift_pdfkit) or extraction failed",
                }

    # Optional OCR augmentation for scan-heavy documents (macOS Vision via Swift).
    low_text_pages = [p for p in pages if p.get("chars", 0) < 40]
    if pages and (len(low_text_pages) / len(pages)) > 0.8:
        ocr_script = Path(__file__).resolve().parent / "pdf_ocr_extract.swift"
        if ocr_script.exists():
            ocr_status = "attempted"
            try:
                proc = subprocess.run(
                    ["swift", str(ocr_script), str(pdf_path), "30"],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=300,
                )
                if proc.returncode == 0 and proc.stdout.strip():
                    parsed = json.loads(proc.stdout)
                    if isinstance(parsed, dict) and parsed.get("status") == "ok":
                        ocr_pages = {p.get("page"): p for p in parsed.get("pages", [])}
                        for page in pages:
                            if page.get("chars", 0) < 40:
                                ocr_item = ocr_pages.get(page.get("page"))
                                if ocr_item and ocr_item.get("chars", 0) > page.get("chars", 0):
                                    page["text"] = ocr_item.get("text", "")
                                    page["chars"] = ocr_item.get("chars", 0)
                        engine = f"{engine}+swift_vision_ocr"
                        ocr_status = "success"
                    else:
                        ocr_status = "failed"
                        ocr_error = "OCR output invalid"
                else:
                    ocr_status = "failed"
                    ocr_error = proc.stderr.strip() or proc.stdout.strip() or "OCR execution failed"
            except Exception:
                ocr_status = "failed"
                ocr_error = "OCR runtime exception"

    return {"engine": engine, "status": "ok", "pages": pages, "ocr_status": ocr_status, "ocr_error": ocr_error}


def stage_ingest(config: Dict[str, Any], project_root: Path, log_file: Path) -> Dict[str, Any]:
    records: List[Dict[str, Any]] = []
    missing: List[str] = []
    for p in config["input_files"]:
        fp = Path(p)
        exists = fp.exists()
        item = {
            "path": str(fp),
            "name": fp.name,
            "period": parse_year_from_name(fp.name),
            "ext": fp.suffix.lower(),
            "exists": exists,
            "size_bytes": fp.stat().st_size if exists else None,
            "md5": file_md5(fp) if exists else None,
            "parser_status": "pending",
        }
        if not exists:
            missing.append(str(fp))
        records.append(item)

    out = {
        "project_id": config["project_id"],
        "company_name": config["company_name"],
        "generated_at": utc_now(),
        "records": records,
    }
    out_path = project_root / "data" / "ingest_manifest.json"
    dump_json(out_path, out)
    log_event(
        log_file,
        "ingest",
        "error" if missing else "info",
        "Ingest manifest generated",
        {"output": str(out_path), "total_files": len(records), "missing_files": missing},
    )
    return out


def stage_extract(ingest_manifest: Dict[str, Any], project_root: Path, log_file: Path) -> Dict[str, Any]:
    alias_cfg = load_json(project_root / "rules" / "mapping" / "aliases.json")
    alias_pairs = alias_cfg.get("aliases", [])

    num_pat = re.compile(r"[-(]?\d[\d,]*(?:\.\d+)?\)?")
    candidate_rows: List[Dict[str, Any]] = []
    file_stats: List[Dict[str, Any]] = []

    for rec in ingest_manifest.get("records", []):
        if not rec.get("exists") or rec.get("ext") != ".pdf":
            continue

        pdf_path = Path(rec["path"])
        ext_res = extract_pdf_pages(pdf_path)
        if ext_res["status"] != "ok":
            file_stats.append(
                {
                    "file": str(pdf_path),
                    "period": rec.get("period"),
                    "engine": ext_res.get("engine"),
                    "status": "failed",
                    "error": ext_res.get("error"),
                    "page_count": 0,
                    "low_text_pages": 0,
                    "low_text_ratio": None,
                }
            )
            continue

        pages = ext_res["pages"]
        low_text_pages = 0
        for page in pages:
            text = page.get("text", "")
            if page.get("chars", 0) < 40:
                low_text_pages += 1
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
            for line in lines:
                line_norm = normalize_for_match(line)
                for pair in alias_pairs:
                    raw = pair.get("raw", "")
                    standard = pair.get("standard", "")
                    raw_norm = normalize_for_match(raw)
                    if raw_norm and raw_norm in line_norm:
                        nums = num_pat.findall(line)
                        if not nums:
                            continue
                        val = normalize_number(nums[0])
                        if val is None:
                            continue
                        candidate_rows.append(
                            {
                                "file": str(pdf_path),
                                "period": rec.get("period"),
                                "page": page["page"],
                                "raw_subject": raw,
                                "standard_subject": standard,
                                "amount": val,
                                "confidence": 0.9 if page.get("chars", 0) >= 40 else 0.45,
                                "line_sample": line[:200],
                            }
                        )

        page_count = len(pages)
        file_stats.append(
            {
                "file": str(pdf_path),
                "period": rec.get("period"),
                "engine": ext_res.get("engine"),
                "status": "ok",
                "ocr_status": ext_res.get("ocr_status"),
                "ocr_error": ext_res.get("ocr_error"),
                "page_count": page_count,
                "low_text_pages": low_text_pages,
                "low_text_ratio": (low_text_pages / page_count) if page_count else None,
            }
        )

    per_period: Dict[str, Dict[str, float]] = {}
    for row in candidate_rows:
        period = row.get("period") or "unknown"
        subject = row.get("standard_subject")
        if period not in per_period:
            per_period[period] = {}
        if subject not in per_period[period]:
            per_period[period][subject] = row["amount"]

    out = {
        "generated_at": utc_now(),
        "file_stats": file_stats,
        "candidate_count": len(candidate_rows),
        "candidates": candidate_rows,
        "per_period_subject_values": per_period,
    }

    out_path = project_root / "outputs" / "extracted_subject_values.json"
    dump_json(out_path, out)
    log_event(
        log_file,
        "extract",
        "info",
        "Extraction results generated",
        {
            "output": str(out_path),
            "candidate_count": len(candidate_rows),
            "files_processed": len(file_stats),
        },
    )
    return out


def stage_quality(
    ingest_manifest: Dict[str, Any], extract_result: Dict[str, Any], project_root: Path, log_file: Path
) -> Dict[str, Any]:
    issues: List[Dict[str, Any]] = []

    for record in ingest_manifest.get("records", []):
        if record.get("exists") is not True:
            issues.append(
                {
                    "issue_type": "missing_file",
                    "severity": "error",
                    "file": record.get("path"),
                    "note": "Source file does not exist",
                }
            )

    file_stats = extract_result.get("file_stats", [])
    if not file_stats:
        issues.append(
            {
                "issue_type": "extract_unavailable",
                "severity": "error",
                "file": None,
                "note": "No file extraction stats generated",
            }
        )

    for fs in file_stats:
        if fs.get("status") != "ok":
            issues.append(
                {
                    "issue_type": "extract_failed",
                    "severity": "error",
                    "file": fs.get("file"),
                    "note": fs.get("error", "PDF extraction failed"),
                }
            )
            continue

        if fs.get("ocr_status") == "failed":
            issues.append(
                {
                    "issue_type": "ocr_failed",
                    "severity": "warn",
                    "file": fs.get("file"),
                    "note": fs.get("ocr_error") or "OCR attempt failed",
                    "action": "Check OCR runtime environment and retry",
                }
            )

        low_ratio = fs.get("low_text_ratio")
        if isinstance(low_ratio, float) and low_ratio > 0.5:
            issues.append(
                {
                    "issue_type": "scan_heavy_pdf",
                    "severity": "warn",
                    "file": fs.get("file"),
                    "note": f"Low-text pages ratio is high: {low_ratio:.2%}",
                    "action": "Run OCR extraction for scanned pages and re-run reconciliation",
                }
            )

    if extract_result.get("candidate_count", 0) == 0:
        issues.append(
            {
                "issue_type": "no_structured_values",
                "severity": "error",
                "file": None,
                "note": "No subject-value candidates extracted from PDFs",
                "action": "Load OCR output or supplemental structured sources (workpapers) for mapping",
            }
        )

    out = {
        "generated_at": utc_now(),
        "issue_count": len(issues),
        "issues": issues,
    }
    out_path = project_root / "outputs" / "quality_issues.json"
    dump_json(out_path, out)
    log_event(
        log_file,
        "quality",
        "warn" if issues else "info",
        "Quality issue list generated",
        {"output": str(out_path), "issue_count": len(issues)},
    )
    return out


def stage_mapping(extract_result: Dict[str, Any], project_root: Path, log_file: Path) -> Dict[str, Any]:
    subject_system = load_json(project_root / "rules" / "mapping" / "subject_system.json")
    aliases = load_json(project_root / "rules" / "mapping" / "aliases.json")
    table_counts = {k: len(v) for k, v in subject_system.get("tables", {}).items()}

    mapped_count = 0
    unmapped_items: List[str] = []
    for row in extract_result.get("candidates", []):
        if row.get("standard_subject"):
            mapped_count += 1
        else:
            raw = row.get("raw_subject")
            if raw and raw not in unmapped_items:
                unmapped_items.append(raw)

    out = {
        "generated_at": utc_now(),
        "table_subject_counts": table_counts,
        "alias_count": len(aliases.get("aliases", [])),
        "mapped_count": mapped_count,
        "unmapped_items": unmapped_items,
    }
    out_path = project_root / "outputs" / "mapping_coverage.json"
    dump_json(out_path, out)
    log_event(
        log_file,
        "mapping",
        "info",
        "Mapping coverage generated",
        {
            "output": str(out_path),
            "alias_count": out["alias_count"],
            "mapped_count": mapped_count,
            "unmapped_count": len(unmapped_items),
        },
    )
    return out


def stage_variance(config: Dict[str, Any], project_root: Path, log_file: Path) -> Dict[str, Any]:
    out = {
        "generated_at": utc_now(),
        "source_priority": config.get("source_priority", []),
        "variances": [],
        "note": "Only audit reports loaded. Cross-source variance checks will run after loading workpapers/rating reports.",
    }
    out_path = project_root / "outputs" / "variance_issues.json"
    dump_json(out_path, out)
    log_event(
        log_file,
        "variance",
        "info",
        "Variance issue list initialized",
        {"output": str(out_path), "variance_count": 0},
    )
    return out


def evaluate_rule(rule_id: str, values: Dict[str, float], tol_abs: float) -> Dict[str, Any]:
    if rule_id == "BS_EQ_001":
        required = ["total_assets", "total_liabilities", "total_equity"]
        if not all(k in values for k in required):
            return {"status": "pending_data", "diff": None, "required": required}
        left = values["total_assets"]
        right = values["total_liabilities"] + values["total_equity"]
        diff = left - right
        return {"status": "pass" if abs(diff) <= tol_abs else "fail", "diff": diff, "required": required}

    if rule_id == "BS_EQ_002":
        required = ["total_assets", "total_current_assets", "total_noncurrent_assets"]
        if not all(k in values for k in required):
            return {"status": "pending_data", "diff": None, "required": required}
        diff = values["total_assets"] - (values["total_current_assets"] + values["total_noncurrent_assets"])
        return {"status": "pass" if abs(diff) <= tol_abs else "fail", "diff": diff, "required": required}

    if rule_id == "BS_EQ_003":
        required = ["total_liabilities", "total_current_liabilities", "total_noncurrent_liabilities"]
        if not all(k in values for k in required):
            return {"status": "pending_data", "diff": None, "required": required}
        diff = values["total_liabilities"] - (
            values["total_current_liabilities"] + values["total_noncurrent_liabilities"]
        )
        return {"status": "pass" if abs(diff) <= tol_abs else "fail", "diff": diff, "required": required}

    if rule_id == "CF_EQ_001":
        required = ["net_cash_change", "net_cash_operating", "net_cash_investment", "net_cash_financing"]
        if not all(k in values for k in required):
            return {"status": "pending_data", "diff": None, "required": required}
        fx_impact = values.get("fx_impact", 0.0)
        diff = values["net_cash_change"] - (
            values["net_cash_operating"] + values["net_cash_investment"] + values["net_cash_financing"] + fx_impact
        )
        return {"status": "pass" if abs(diff) <= tol_abs else "fail", "diff": diff, "required": required}

    if rule_id == "RF_EQ_001":
        required = ["opening_cash_balance", "net_cash_change", "ending_cash_balance"]
        if not all(k in values for k in required):
            return {"status": "pending_data", "diff": None, "required": required}
        diff = values["ending_cash_balance"] - (values["opening_cash_balance"] + values["net_cash_change"])
        return {"status": "pass" if abs(diff) <= tol_abs else "fail", "diff": diff, "required": required}

    return {"status": "pending_data", "diff": None, "required": []}


def stage_recon(config: Dict[str, Any], extract_result: Dict[str, Any], project_root: Path, log_file: Path) -> Dict[str, Any]:
    recon = load_json(project_root / "rules" / "recon" / "rules.json")
    per_period = extract_result.get("per_period_subject_values", {})
    tol_abs = float(config.get("tolerance", {}).get("abs", 0.5))

    checks: List[Dict[str, Any]] = []
    for period, values in per_period.items():
        for family in recon.get("families", []):
            for rule in family.get("rules", []):
                eval_res = evaluate_rule(rule["id"], values, tol_abs)
                checks.append(
                    {
                        "period": period,
                        "id": rule["id"],
                        "family": family["family"],
                        "name": rule["name"],
                        "status": eval_res["status"],
                        "diff": eval_res["diff"],
                        "required_fields": eval_res["required"],
                        "severity": rule.get("severity", "warn"),
                        "tolerance_abs": tol_abs,
                    }
                )

    status_counts = {"pass": 0, "fail": 0, "pending_data": 0}
    failed_checks: List[Dict[str, Any]] = []
    for chk in checks:
        status_counts[chk["status"]] = status_counts.get(chk["status"], 0) + 1
        if chk["status"] == "fail":
            failed_checks.append(chk)

    out = {
        "generated_at": utc_now(),
        "check_count": len(checks),
        "status_counts": status_counts,
        "failed_checks": failed_checks,
        "checks": checks,
    }
    out_path = project_root / "outputs" / "recon_checklist.json"
    dump_json(out_path, out)
    log_event(
        log_file,
        "recon",
        "warn" if failed_checks else "info",
        "Reconciliation execution completed",
        {
            "output": str(out_path),
            "check_count": len(checks),
            "fail_count": len(failed_checks),
            "pending_count": status_counts.get("pending_data", 0),
        },
    )
    return out


def stage_analysis(extract_result: Dict[str, Any], project_root: Path, log_file: Path) -> Dict[str, Any]:
    indicators = load_json(project_root / "config" / "indicator_set.json")
    anomaly_rules = load_json(project_root / "rules" / "anomaly" / "rules.json")
    per_period = extract_result.get("per_period_subject_values", {})

    periods = sorted([p for p in per_period.keys() if p.isdigit()])
    indicator_values: Dict[str, Dict[str, float]] = {}

    for period in periods:
        vals = per_period.get(period, {})
        bucket: Dict[str, float] = {}

        total_assets = vals.get("total_assets")
        total_liabilities = vals.get("total_liabilities")
        net_profit = vals.get("net_profit")
        revenue = vals.get("operating_revenue")

        if total_assets and total_liabilities is not None:
            bucket["debt_to_asset_ratio"] = total_liabilities / total_assets if total_assets != 0 else 0.0
        if net_profit is not None and revenue:
            bucket["sales_net_margin"] = net_profit / revenue if revenue != 0 else 0.0

        indicator_values[period] = bucket

    anomalies: List[Dict[str, Any]] = []
    if len(periods) >= 2:
        p0 = periods[-2]
        p1 = periods[-1]
        v0 = per_period.get(p0, {}).get("operating_revenue")
        v1 = per_period.get(p1, {}).get("operating_revenue")
        if v0 not in (None, 0) and v1 is not None:
            yoy = (v1 / v0) - 1
            if abs(yoy) > 0.3:
                anomalies.append(
                    {
                        "rule_id": "AN_TR_001",
                        "metric": "operating_revenue_yoy",
                        "period": p1,
                        "value": yoy,
                        "severity": "warn",
                    }
                )

    out = {
        "generated_at": utc_now(),
        "indicator_count_configured": len(indicators.get("indicators", [])),
        "anomaly_rule_count": len(anomaly_rules.get("rules", [])),
        "periods_detected": periods,
        "indicator_values": indicator_values,
        "anomalies": anomalies,
        "analysis_status": "partial" if per_period else "pending_data",
    }

    out_path = project_root / "outputs" / "analysis_plan.json"
    dump_json(out_path, out)
    log_event(
        log_file,
        "analysis",
        "warn" if anomalies else "info",
        "Analysis execution completed",
        {
            "output": str(out_path),
            "periods_detected": periods,
            "anomaly_count": len(anomalies),
        },
    )
    return out


def stage_risk(
    config: Dict[str, Any],
    quality_result: Dict[str, Any],
    recon_result: Dict[str, Any],
    analysis_result: Dict[str, Any],
    project_root: Path,
    log_file: Path,
) -> Dict[str, Any]:
    risk_rules = load_json(project_root / "rules" / "risk" / "rules.json")

    quality_penalty = min(25, quality_result.get("issue_count", 0) * 5)
    recon_penalty = min(40, len(recon_result.get("failed_checks", [])) * 10)
    anomaly_penalty = min(20, len(analysis_result.get("anomalies", [])) * 10)
    base_score = 20
    total_score = min(100, base_score + quality_penalty + recon_penalty + anomaly_penalty)

    grade = "medium"
    grading = risk_rules.get("grading", {})
    for label, rng in grading.items():
        if rng.get("min", 0) <= total_score <= rng.get("max", 100):
            grade = label
            break

    report_lines = [
        "# Credit Risk Report (MVP)",
        "",
        f"- Project ID: {config['project_id']}",
        f"- Company: {config['company_name']}",
        f"- Generated At (UTC): {utc_now()}",
        "",
        "## Data Source and Priority",
        f"- Priority: {', '.join(config.get('source_priority', []))}",
        "",
        "## Data Quality Summary",
        f"- Issue count: {quality_result.get('issue_count', 0)}",
        "- Note: scan-heavy PDFs may limit direct text extraction and downstream reconciliation depth.",
        "",
        "## Reconciliation Summary",
        f"- Checks: {recon_result.get('check_count', 0)}",
        f"- Failed checks: {len(recon_result.get('failed_checks', []))}",
        "",
        "## Variance Disclosure",
        "- Cross-source variance is pending additional source extraction.",
        "",
        "## Risk Score",
        f"- Score: {total_score}",
        f"- Grade: {grade}",
        "",
        "## Evidence Summary",
        f"- Quality penalty: {quality_penalty}",
        f"- Recon penalty: {recon_penalty}",
        f"- Anomaly penalty: {anomaly_penalty}",
        "",
        "## Next Actions",
        "- Add OCR for scanned pages.",
        "- Load supplemental sources for cross-source variance checks.",
    ]

    report_path = project_root / "outputs" / "risk_report_mvp.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(report_lines), encoding="utf-8")

    out = {
        "generated_at": utc_now(),
        "report_path": str(report_path),
        "score": total_score,
        "grade": grade,
        "status": "generated",
    }

    log_event(
        log_file,
        "risk",
        "info",
        "Risk report generated",
        {
            "output": str(report_path),
            "score": total_score,
            "grade": grade,
            "factor_count": len(risk_rules.get("factors", [])),
        },
    )
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run MVP credit risk pipeline")
    parser.add_argument(
        "--config",
        default="/Users/sijia/Documents/credit_risk_system/config/project_config_shiyan.json",
        help="Path to project config JSON",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Config not found: {config_path}")
        return 1

    config = load_json(config_path)
    project_root = Path("/Users/sijia/Documents/credit_risk_system")
    log_file = project_root / "logs" / "pipeline_events.jsonl"

    log_event(log_file, "pipeline", "info", "Pipeline start", {"config": str(config_path)})

    ingest_manifest = stage_ingest(config, project_root, log_file)
    extract_result = stage_extract(ingest_manifest, project_root, log_file)
    quality_result = stage_quality(ingest_manifest, extract_result, project_root, log_file)
    stage_mapping(extract_result, project_root, log_file)
    stage_variance(config, project_root, log_file)
    recon_result = stage_recon(config, extract_result, project_root, log_file)
    analysis_result = stage_analysis(extract_result, project_root, log_file)
    stage_risk(config, quality_result, recon_result, analysis_result, project_root, log_file)

    log_event(log_file, "pipeline", "info", "Pipeline completed", {"project_id": config["project_id"]})
    print("Pipeline run completed. Check outputs/ and logs/.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
