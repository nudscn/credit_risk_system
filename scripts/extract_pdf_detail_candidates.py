#!/usr/bin/env python3
"""Extract detail candidates from searchable PDFs (independent stage).

Design goals:
- Keep this script independent from stage-1/stage-2 logic.
- Parse project PDFs and output reviewable candidate facts only.
- Preserve evidence (file/page/snippet) for manual confirmation.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RULES_PATH = PROJECT_ROOT / "config" / "detail_extraction_rules_v1.json"


def normalize_project_id(project_id: str) -> str:
    text = re.sub(r"[^0-9A-Za-z._-]+", "_", (project_id or "").strip())
    return text or "default_project"


def resolve_path(path_text: str) -> Path:
    p = Path(path_text)
    return p if p.is_absolute() else PROJECT_ROOT / p


def load_rules(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"version": "v1", "fields": []}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"version": "v1", "fields": []}


def list_pdf_files(search_dirs: List[Path]) -> List[Path]:
    out: List[Path] = []
    seen = set()
    for d in search_dirs:
        if not d.exists() or not d.is_dir():
            continue
        for p in d.rglob("*.pdf"):
            if not p.is_file():
                continue
            key = str(p.resolve())
            if key in seen:
                continue
            seen.add(key)
            out.append(p)
    return sorted(out)


def extract_pdf_pages_text(pdf_path: Path) -> List[Tuple[int, str]]:
    """Return [(page_no_1based, text)] with multi-backend fallback."""
    pages: List[Tuple[int, str]] = []

    # backend 1: pypdf
    try:
        from pypdf import PdfReader  # type: ignore

        reader = PdfReader(str(pdf_path))
        for i, page in enumerate(reader.pages, start=1):
            txt = page.extract_text() or ""
            pages.append((i, txt))
        if any(t.strip() for _, t in pages):
            return pages
    except Exception:
        pass

    # backend 2: pdfplumber
    pages = []
    try:
        import pdfplumber  # type: ignore

        with pdfplumber.open(str(pdf_path)) as doc:
            for i, page in enumerate(doc.pages, start=1):
                txt = page.extract_text() or ""
                pages.append((i, txt))
        return pages
    except Exception:
        return []


def normalize_text(s: str) -> str:
    """Normalize for robust keyword matching across layout variance."""
    text = str(s or "")
    # remove whitespace and common separators that often break chinese terms
    text = re.sub(r"[\s\u3000]+", "", text)
    text = re.sub(r"[·•|│┃—–-]+", "", text)
    return text.lower()


def text_variants(s: str) -> List[str]:
    """Return original + common mojibake variants for robust matching."""
    base = str(s or "")
    out = [base]
    for codec in ("gbk", "gb18030", "latin1"):
        try:
            out.append(base.encode("utf-8").decode(codec, errors="ignore"))
        except Exception:
            pass
    # de-duplicate while preserving order
    seen = set()
    uniq: List[str] = []
    for x in out:
        if x in seen:
            continue
        seen.add(x)
        uniq.append(x)
    return uniq


def spaced_hanzi_pattern(text: str) -> str:
    """Convert Chinese text to regex allowing optional spaces/newlines between chars."""
    chars = list(text)
    return r"[\s\u3000]*".join(re.escape(ch) for ch in chars)


def loose_keyword_match(text: str, keyword: str) -> bool:
    if not keyword:
        return False
    # direct normalized containment first
    if normalize_text(keyword) in normalize_text(text):
        return True
    # then char-wise loose match for chinese words
    if re.search(r"[\u4e00-\u9fff]", keyword):
        pat = spaced_hanzi_pattern(keyword)
        return re.search(pat, text, flags=re.IGNORECASE | re.MULTILINE) is not None
    return False


def keyword_hit_count(text: str, keywords: Iterable[str]) -> int:
    hit = 0
    for k in keywords:
        matched = False
        for kv in text_variants(str(k)):
            if loose_keyword_match(text, kv):
                matched = True
                break
        if matched:
            hit += 1
    return hit


def parse_number(raw: str) -> Optional[float]:
    s = str(raw or "").strip().replace(",", "").replace("，", "")
    if not s:
        return None
    neg = False
    if (s.startswith("(") and s.endswith(")")) or (s.startswith("（") and s.endswith("）")):
        neg = True
        s = s[1:-1].strip()
    if s.startswith("-"):
        neg = True
        s = s[1:].strip()
    try:
        v = float(s)
        return -v if neg else v
    except Exception:
        return None


def infer_year_from_filename(pdf_path: Path) -> Optional[str]:
    # prefer report year marker in file name, e.g. "...2024年年度报告.pdf"
    m = re.search(r"(20\d{2})年年度报告", pdf_path.name)
    if m:
        return m.group(1)
    # fallback: first 20xx token
    m = re.search(r"(20\d{2})", pdf_path.name)
    return m.group(1) if m else None


def build_snippet(text: str, start: int, end: int, radius: int = 80) -> str:
    s = max(0, start - radius)
    e = min(len(text), end + radius)
    return text[s:e].replace("\n", " ").strip()


def find_matches(patterns: List[str], text: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    expanded: List[str] = []
    for pat in patterns:
        expanded.extend(text_variants(pat))

    # de-duplicate patterns
    dedup: List[str] = []
    seen = set()
    for pat in expanded:
        if pat in seen:
            continue
        seen.add(pat)
        dedup.append(pat)

    for pat in dedup:
        # build a loose variant that allows whitespace between chinese characters
        loose_pat = re.sub(
            r"[\u4e00-\u9fff]+",
            lambda m: spaced_hanzi_pattern(m.group(0)),
            pat,
        )
        for candidate_pat in [pat, loose_pat]:
            try:
                rx = re.compile(candidate_pat, flags=re.IGNORECASE | re.MULTILINE)
            except re.error:
                continue
            for m in rx.finditer(text):
                gd = m.groupdict()
                if "value" in gd:
                    raw_value = gd.get("value", "")
                elif m.groups():
                    raw_value = m.group(1)
                else:
                    raw_value = m.group(0)
                year = gd.get("year", "") if gd else ""
                out.append(
                    {
                        "raw_value": str(raw_value or "").strip(),
                        "year": str(year or "").strip(),
                        "start": m.start(),
                        "end": m.end(),
                        "pattern": pat,
                    }
                )
    return out


def extract_candidates_from_pages(
    pages: List[Tuple[int, str]],
    rules: Dict[str, Any],
    pdf_path: Path,
) -> List[Dict[str, Any]]:
    fields = rules.get("fields", [])
    out: List[Dict[str, Any]] = []
    fallback_year = infer_year_from_filename(pdf_path)

    for field in fields:
        if not isinstance(field, dict):
            continue
        field_id = str(field.get("field_id", "")).strip()
        field_name = str(field.get("field_name", "")).strip() or field_id
        keywords = [str(x).strip() for x in field.get("keywords", []) if str(x).strip()]
        patterns = [str(x).strip() for x in field.get("value_patterns", []) if str(x).strip()]
        min_abs_value = float(field.get("min_abs_value", 0) or 0)
        max_abs_value = float(field.get("max_abs_value", 0) or 0)
        if not field_id or not patterns:
            continue

        candidates: List[Dict[str, Any]] = []
        for page_no, text in pages:
            if not text:
                continue
            kw_hits = keyword_hit_count(text, keywords)
            if keywords and kw_hits == 0:
                continue
            matches = find_matches(patterns, text)
            for m in matches:
                value = parse_number(m["raw_value"])
                if value is not None:
                    if min_abs_value > 0 and abs(value) < min_abs_value:
                        continue
                    if max_abs_value > 0 and abs(value) > max_abs_value:
                        continue
                year = m["year"] or fallback_year or ""
                conf = 0.2 + 0.15 * kw_hits + (0.25 if value is not None else 0.0) + (0.15 if year else 0.0)
                conf = min(0.99, round(conf, 3))
                candidates.append(
                    {
                        "field_id": field_id,
                        "field_name": field_name,
                        "year": year,
                        "raw_value": m["raw_value"],
                        "parsed_value": value,
                        "confidence": conf,
                        "keyword_hits": kw_hits,
                        "pattern": m["pattern"],
                        "file": str(pdf_path),
                        "page": page_no,
                        "snippet": build_snippet(text, m["start"], m["end"]),
                    }
                )

        # keep top candidates per (field, year, file)
        key_best: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        for c in candidates:
            key = (c["field_id"], c.get("year", ""), c["file"])
            old = key_best.get(key)
            if old is None or c["confidence"] > old["confidence"]:
                key_best[key] = c
        out.extend(key_best.values())

    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract detail candidates from project PDFs")
    parser.add_argument("--project-id", required=True, help="Project identifier")
    parser.add_argument(
        "--input-dir",
        default="",
        help="Input directory override; default scans inputs/<project_id>/ then inputs/",
    )
    parser.add_argument(
        "--rules",
        default=str(DEFAULT_RULES_PATH),
        help="Rules JSON path (default: config/detail_extraction_rules_v1.json)",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Output JSON path override; default outputs/<project_id>/<project_id>_detail_candidates.json",
    )
    args = parser.parse_args()

    project_id = normalize_project_id(args.project_id)
    search_dirs = [resolve_path(args.input_dir)] if args.input_dir else [
        PROJECT_ROOT / "inputs" / project_id,
        PROJECT_ROOT / "inputs",
    ]
    rules_path = resolve_path(args.rules)
    rules = load_rules(rules_path)
    pdf_files = list_pdf_files(search_dirs)

    all_candidates: List[Dict[str, Any]] = []
    for pdf_path in pdf_files:
        pages = extract_pdf_pages_text(pdf_path)
        if not pages:
            continue
        all_candidates.extend(extract_candidates_from_pages(pages, rules, pdf_path))

    # sort for deterministic review
    all_candidates.sort(
        key=lambda x: (
            x.get("field_id", ""),
            x.get("year", ""),
            -float(x.get("confidence", 0.0)),
            x.get("file", ""),
            int(x.get("page", 0)),
        )
    )

    out_path = resolve_path(args.output) if args.output else (
        PROJECT_ROOT / "outputs" / project_id / f"{project_id}_detail_candidates.json"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "project_id": project_id,
        "rules_path": str(rules_path),
        "search_dirs": [str(x) for x in search_dirs],
        "pdf_count": len(pdf_files),
        "candidate_count": len(all_candidates),
        "candidates": all_candidates,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Generated: {out_path}")
    print(f"PDF files: {len(pdf_files)}")
    print(f"Candidates: {len(all_candidates)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
