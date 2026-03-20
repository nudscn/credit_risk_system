#!/usr/bin/env python3
"""Local web UI for reviewing workbook sheets and narrative edits.

Run:
  python webapp/server.py --host 127.0.0.1 --port 8787
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, quote, unquote, urlparse

from openpyxl import Workbook, load_workbook

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROJECT_ID = "zhonggang_luonai"
DEFAULT_YEAR_COUNT = 3

SHEET_GROUPS = [
    {"id": "bs", "label": "资产负债表", "candidates": ["资产负债表", "资产负债表 "]},
    {"id": "is", "label": "利润表", "candidates": ["利润表", "利润表 "]},
    {"id": "cf", "label": "现金流量表", "candidates": ["现金流量表", "现金流量表 "]},
    {"id": "ratio", "label": "财务指标表", "candidates": ["财务比率", "财务指标", "财务指标表"]},
]

ASSET_ANALYSIS_SHEETS = {
    "scale": ["分析_规模变化_资产类"],
    "structure": ["分析_结构占比_资产类"],
}
LIABILITY_ANALYSIS_SHEETS = {
    "scale": ["分析_规模变化_负债类"],
    "structure": ["分析_结构占比_负债类"],
}
ANALYSIS_CODE_REDIRECTS = {
    "asset_analysis": {
        "BS028": "BS056",  # 非流动资产 <- 非流动资产合计
    },
    "liability_analysis": {
        "BS058": "BS089",  # 流动负债 <- 流动负债合计
        "BS090": "BS102",  # 非流动负债 <- 非流动负债合计
    },
}

INCOME_RULEBOOK_PATH = PROJECT_ROOT / "config" / "income_profit_rulebook_skeleton.xlsx"
INCOME_RULEBOOK_FALLBACK_PATH = PROJECT_ROOT / "config" / "rulebook.xlsx"
RATIO_RULEBOOK_PATH = PROJECT_ROOT / "config" / "ratio_analysis_rulebook.xlsx"
KEY_RATIO_RULEBOOK_PATH = PROJECT_ROOT / "config" / "key_ratio_rulebook.xlsx"
DEFAULT_INCOME_SPECIAL_ITEMS = [
    {"node_id": "2.1.2.1", "label": "其他收益（政府补助等）", "code_candidates": [], "name_keywords": ["其他收益"]},
    {"node_id": "2.1.2.2", "label": "信用减值损失", "code_candidates": [], "name_keywords": ["信用减值损失"]},
    {"node_id": "2.1.2.3", "label": "资产减值损失", "code_candidates": [], "name_keywords": ["资产减值损失"]},
    {"node_id": "2.1.2.4", "label": "待判定收益项", "code_candidates": [], "name_keywords": []},
    {"node_id": "2.1.2.5", "label": "投资收益（待判定）", "code_candidates": [], "name_keywords": ["投资收益", "投资净收益"]},
    {"node_id": "2.1.2.6", "label": "公允价值变动收益（待判定）", "code_candidates": [], "name_keywords": ["公允价值变动收益", "公允价值变动净收益"]},
]
DEFAULT_RECURRING_KEYS = {"2.1.1", "2.1.2.1", "2.1.2.2", "2.1.2.3", "2.1.2.5", "2.1.2.6"}
DEFAULT_NONREC_KEYS = {"2.2.1", "2.2.2"}
DEFAULT_INCOME_FORMULAS = [
    {"node_id": "3.1", "formula": "SUM(2.1.1,2.1.2.1,2.1.2.2,2.1.2.3,2.1.2.5,2.1.2.6)"},
    {"node_id": "3.2", "formula": "SUM(2.2.1,2.2.2)"},
    {"node_id": "3.3", "formula": "SUB(3.1,3.2)"},
]
DEFAULT_INCOME_TREE = [
    {"node_id": "1", "parent_id": "", "label": "营业收入分析", "node_type": "fixed"},
    {"node_id": "1.1", "parent_id": "1", "label": "营业收入总额", "node_type": "fixed"},
    {
        "node_id": "",
        "parent_id": "1",
        "label": "",
        "node_type": "template",
        "template_name": "revenue_segment",
        "node_id_prefix": "1.",
        "start_index": 2,
        "label_suffix": "收入",
    },
    {"node_id": "2", "parent_id": "", "label": "收益贡献分析", "node_type": "fixed"},
    {"node_id": "2.1", "parent_id": "2", "label": "经常性收益分析", "node_type": "fixed"},
    {"node_id": "2.1.1", "parent_id": "2.1", "label": "主营业务毛利（自动汇总）", "node_type": "fixed"},
    {
        "node_id": "",
        "parent_id": "2.1.1",
        "label": "",
        "node_type": "template",
        "template_name": "gross_segment",
        "node_id_prefix": "2.1.1.",
        "start_index": 1,
        "label_suffix": "毛利",
    },
    {"node_id": "2.1.2", "parent_id": "2.1", "label": "其他经营收益", "node_type": "fixed"},
    {"node_id": "2.1.2.1", "parent_id": "2.1.2", "label": "其他收益（政府补助等）", "node_type": "fixed"},
    {"node_id": "2.1.2.2", "parent_id": "2.1.2", "label": "信用减值损失", "node_type": "fixed"},
    {"node_id": "2.1.2.3", "parent_id": "2.1.2", "label": "资产减值损失", "node_type": "fixed"},
    {"node_id": "2.1.2.4", "parent_id": "2.1.2", "label": "待判定收益项", "node_type": "fixed"},
    {"node_id": "2.1.2.5", "parent_id": "2.1.2", "label": "投资收益（待判定）", "node_type": "fixed"},
    {"node_id": "2.1.2.6", "parent_id": "2.1.2", "label": "公允价值变动收益（待判定）", "node_type": "fixed"},
    {"node_id": "2.2", "parent_id": "2", "label": "非经常性收益分析", "node_type": "fixed"},
    {"node_id": "2.2.1", "parent_id": "2.2", "label": "资产处置收益", "node_type": "fixed"},
    {"node_id": "2.2.2", "parent_id": "2.2", "label": "营业外收支净额（自动汇总）", "node_type": "fixed"},
    {"node_id": "3", "parent_id": "", "label": "分析输出", "node_type": "fixed"},
    {"node_id": "3.1", "parent_id": "3", "label": "经常性收益小计（自动汇总）", "node_type": "fixed"},
    {"node_id": "3.2", "parent_id": "3", "label": "非经常性收益小计（自动汇总）", "node_type": "fixed"},
    {"node_id": "3.3", "parent_id": "3", "label": "收益结构判断（自动+手动）", "node_type": "fixed"},
]
DEFAULT_RATIO_TREE = [
    {"node_id": "1", "parent_id": "", "label_zh": "偿债能力分析", "node_type": "group", "indicator_id": "", "enabled": 1, "sort_order": 10},
    {"node_id": "2", "parent_id": "", "label_zh": "盈利能力分析", "node_type": "group", "indicator_id": "", "enabled": 1, "sort_order": 20},
    {"node_id": "2.0", "parent_id": "2", "label_zh": "ROE杜邦专题", "node_type": "topic", "indicator_id": "topic_roe_dupont", "enabled": 1, "sort_order": 20.5},
    {"node_id": "2.6", "parent_id": "2", "label_zh": "毛利率贡献专题", "node_type": "topic", "indicator_id": "topic_gross_margin", "enabled": 1, "sort_order": 26},
    {"node_id": "3", "parent_id": "", "label_zh": "营运能力分析", "node_type": "group", "indicator_id": "", "enabled": 1, "sort_order": 30},
    {"node_id": "4", "parent_id": "", "label_zh": "现金流与结构分析", "node_type": "group", "indicator_id": "", "enabled": 1, "sort_order": 40},
]
KEY_RATIO_IDS = {"K1", "K2"}

DEFAULT_TEXT_TEMPLATES = {
    "sheet_auto_no_latest": "{name}：最新年度暂无数值，建议补录后再分析。",
    "sheet_auto_one_year": "{name}：{latest}年为{latest_val}。",
    "sheet_auto_prev_missing": "{name}：{latest}年为{latest_val}。",
    "sheet_auto_two_year": "{name}：{latest}年为{latest_val}，较{prev}年{direction}{abs_diff}（{abs_pct}%）。",
    "income_segment_value": "{label}：{y1}年{v1}万元，{y2}年{v2}万元，{y3}年{v3}万元；{y2}较{y1}{t21}，{y3}较{y2}{t32}。",
    "income_segment_share": "{ratio_label}：{y1}年{r1}，{y2}年{r2}，{y3}年{r3}；{y2}较{y1}{rt21}，{y3}较{y2}{rt32}。",
    "ratio_indicator_value": "{name}：{y1}年{v1}{unit}，{y2}年{v2}{unit}，{y3}年{v3}{unit}。",
    "ratio_indicator_trend": "{y2}较{y1}{judgement21}（变动{delta21}{unit2}）；{y3}较{y2}{judgement32}（变动{delta32}{unit2}）。",
    "key_roe_factors": "净利率：{y1}年{nm1}，{y2}年{nm2}，{y3}年{nm3}；总资产周转率：{y1}年{at1}，{y2}年{at2}，{y3}年{at3}；权益乘数：{y1}年{em1}，{y2}年{em2}，{y3}年{em3}。",
    "key_gm_top_segments": "最新年度分项贡献（按收入占比/分项毛利率）：{top_txt}。",
}

DEFAULT_KEY_DRIVER_THRESHOLDS = {
    "significant_abs_contrib": 0.30,
    "single_driver_share": 0.40,
    "dual_driver_share_sum": 0.70,
    "dual_driver_each_min": 0.20,
    "delta_stable_pp": 2.00,
}
_KEY_RATIO_RULES_CACHE: Optional[Dict[str, Any]] = None


def _render_template(template: str, values: Dict[str, Any]) -> str:
    txt = str(template or "")

    def repl(match: re.Match) -> str:
        key = match.group(1)
        val = values.get(key)
        if val is None:
            return "待补充"
        return str(val)

    return re.sub(r"\{([A-Za-z0-9_]+)\}", repl, txt).strip()


def normalize_project_id(project_id: str) -> str:
    text = re.sub(r"[^0-9A-Za-z._-]+", "_", (project_id or "").strip())
    return text or "default_project"


def workbook_path(project_id: str) -> Path:
    """Resolve workbook path by convention and fallback."""
    pid = normalize_project_id(project_id)
    out_dir = PROJECT_ROOT / "outputs" / pid
    preferred = out_dir / f"{pid}_项目主文件.xlsx"
    if preferred.exists():
        return preferred

    # Fallback to commonly used names.
    for name in ["基础数据_主文件.xlsx", f"{pid}_项目主文件_年份校验.xlsx"]:
        p = out_dir / name
        if p.exists():
            return p

    # Last resort: first xlsx in the project output folder.
    if out_dir.exists():
        for p in sorted(out_dir.glob("*.xlsx")):
            return p
    return preferred


def store_path(project_id: str) -> Path:
    pid = normalize_project_id(project_id)
    return PROJECT_ROOT / "data" / pid / "narrative_store.json"


def normalize_num(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "").replace("，", "")
    if not text:
        return None

    # Support accounting negatives with parentheses, including full-width symbols.
    negative = False
    if (text.startswith("(") and text.endswith(")")) or (text.startswith("（") and text.endswith("）")):
        negative = True
        text = text[1:-1].strip()
    if text.startswith("-"):
        negative = True
        text = text[1:].strip()

    try:
        out = float(text)
    except Exception:
        return None
    return -out if negative else out


def get_sheet_by_loose_name(wb, candidates: List[str]):
    raw_map = {str(name).strip(): name for name in wb.sheetnames}
    for cand in candidates:
        key = str(cand).strip()
        if key in raw_map:
            return wb[raw_map[key]]
    return None


def parse_years_from_sheet(ws) -> List[str]:
    """Find years from header rows. Prefer row 3, fallback to row 1-6 scan."""
    years: List[str] = []

    def collect_from_row(row_idx: int) -> None:
        for col in range(3, ws.max_column + 1):
            text = str(ws.cell(row_idx, col).value or "")
            m = re.search(r"(20\d{2})", text)
            if not m:
                continue
            y = m.group(1)
            if y not in years:
                years.append(y)

    collect_from_row(3)
    if not years:
        for row_idx in range(1, min(ws.max_row, 6) + 1):
            collect_from_row(row_idx)
            if years:
                break
    return years[:DEFAULT_YEAR_COUNT]


def _safe_float(v: Any, default: float) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _to_pct_threshold(value: float, unit: str) -> float:
    u = str(unit or "").strip().lower()
    if u == "ratio" and abs(value) <= 1:
        return value * 100.0
    return value


def _is_valid_text_cell(text: str) -> bool:
    t = str(text or "").strip()
    if not t:
        return False
    # Guard against mojibake/question-mark placeholders from bad encoding pipelines.
    if "?" in t:
        return False
    return True


def load_income_rules() -> Dict[str, Any]:
    cfg = {
        "yoy_stable_pct": 2.0,
        "share_stable_pp": 2.0,
        "special_items": list(DEFAULT_INCOME_SPECIAL_ITEMS),
        "recurring_keys": set(DEFAULT_RECURRING_KEYS),
        "nonrec_keys": set(DEFAULT_NONREC_KEYS),
        "formula_rules": list(DEFAULT_INCOME_FORMULAS),
        "tree_nodes": list(DEFAULT_INCOME_TREE),
        "text_templates": {
            "income_segment_value": DEFAULT_TEXT_TEMPLATES["income_segment_value"],
            "income_segment_share": DEFAULT_TEXT_TEMPLATES["income_segment_share"],
        },
    }

    candidate_paths = [INCOME_RULEBOOK_PATH, INCOME_RULEBOOK_FALLBACK_PATH]
    for p in candidate_paths:
        if not p.exists():
            continue
        try:
            wb = load_workbook(p, data_only=True)
        except Exception:
            continue

        ws_tr = get_sheet_by_loose_name(wb, ["trend_thresholds", "趋势阈值"])
        if ws_tr is not None:
            headers = {str(ws_tr.cell(1, c).value or "").strip(): c for c in range(1, ws_tr.max_column + 1)}
            c_metric = headers.get("metric_id", 2)
            c_stable = headers.get("stable_threshold", 3)
            c_unit = headers.get("threshold_unit", 4)
            for r in range(2, ws_tr.max_row + 1):
                metric = str(ws_tr.cell(r, c_metric).value or "").strip()
                stable = _safe_float(ws_tr.cell(r, c_stable).value, 0.0)
                unit = str(ws_tr.cell(r, c_unit).value or "").strip()
                if metric == "rev_yoy":
                    cfg["yoy_stable_pct"] = _to_pct_threshold(stable, unit)
                elif metric in {"rev_share", "gp_share_impact"}:
                    cfg["share_stable_pp"] = stable

        ws_map = get_sheet_by_loose_name(wb, ["income_special_items", "收入特殊收益映射"])
        if ws_map is not None:
            headers = {str(ws_map.cell(1, c).value or "").strip(): c for c in range(1, ws_map.max_column + 1)}
            c_node = headers.get("node_id", 1)
            c_label = headers.get("label", 2)
            c_codes = headers.get("code_candidates", 3)
            c_kw = headers.get("name_keywords", 4)
            c_enabled = headers.get("enabled", 5)
            items = []
            for r in range(2, ws_map.max_row + 1):
                node_id = str(ws_map.cell(r, c_node).value or "").strip()
                label = str(ws_map.cell(r, c_label).value or "").strip()
                enabled_raw = str(ws_map.cell(r, c_enabled).value or "1").strip().lower()
                if enabled_raw in {"0", "false", "no"}:
                    continue
                if not node_id or not _is_valid_text_cell(label):
                    continue
                codes = [x.strip() for x in str(ws_map.cell(r, c_codes).value or "").replace(";", ",").split(",") if x.strip()]
                kws = [x.strip() for x in str(ws_map.cell(r, c_kw).value or "").replace(";", ",").split(",") if x.strip()]
                kws = [x for x in kws if _is_valid_text_cell(x)]
                items.append({"node_id": node_id, "label": label, "code_candidates": codes, "name_keywords": kws})
            if items:
                cfg["special_items"] = items

        ws_group = get_sheet_by_loose_name(wb, ["income_grouping", "收入汇总分组"])
        if ws_group is not None:
            headers = {str(ws_group.cell(1, c).value or "").strip(): c for c in range(1, ws_group.max_column + 1)}
            c_type = headers.get("group_type", 1)
            c_nodes = headers.get("node_ids", 2)
            c_enabled = headers.get("enabled", 3)
            rec, nonrec = set(), set()
            for r in range(2, ws_group.max_row + 1):
                gtype = str(ws_group.cell(r, c_type).value or "").strip().lower()
                nodes_raw = str(ws_group.cell(r, c_nodes).value or "").strip()
                enabled_raw = str(ws_group.cell(r, c_enabled).value or "1").strip().lower()
                if enabled_raw in {"0", "false", "no"} or not nodes_raw:
                    continue
                nodes = {x.strip() for x in nodes_raw.replace(";", ",").split(",") if x.strip()}
                if gtype in {"recurring", "经常性"}:
                    rec |= nodes
                elif gtype in {"nonrecurring", "非经常性"}:
                    nonrec |= nodes
            if rec:
                cfg["recurring_keys"] = rec
            if nonrec:
                cfg["nonrec_keys"] = nonrec

        ws_formula = get_sheet_by_loose_name(wb, ["income_formulas", "收入汇总公式"])
        if ws_formula is not None:
            headers = {str(ws_formula.cell(1, c).value or "").strip(): c for c in range(1, ws_formula.max_column + 1)}
            c_node = headers.get("node_id", 1)
            c_formula = headers.get("formula", 2)
            c_enabled = headers.get("enabled", 3)
            formula_rules = []
            for r in range(2, ws_formula.max_row + 1):
                enabled_raw = str(ws_formula.cell(r, c_enabled).value or "1").strip().lower()
                if enabled_raw in {"0", "false", "no"}:
                    continue
                node_id = str(ws_formula.cell(r, c_node).value or "").strip()
                formula = str(ws_formula.cell(r, c_formula).value or "").strip()
                if not node_id or not formula:
                    continue
                formula_rules.append({"node_id": node_id, "formula": formula})
            if formula_rules:
                cfg["formula_rules"] = formula_rules

        ws_tree = get_sheet_by_loose_name(wb, ["income_tree", "收入分析树"])
        if ws_tree is not None:
            default_fixed_by_id = {
                str(x.get("node_id", "")).strip(): x
                for x in DEFAULT_INCOME_TREE
                if str(x.get("node_type", "fixed")).strip().lower() == "fixed" and str(x.get("node_id", "")).strip()
            }
            default_tpl_by_name = {
                str(x.get("template_name", "")).strip(): x
                for x in DEFAULT_INCOME_TREE
                if str(x.get("node_type", "")).strip().lower() == "template"
            }
            headers = {str(ws_tree.cell(1, c).value or "").strip(): c for c in range(1, ws_tree.max_column + 1)}
            c_node = headers.get("node_id", 1)
            c_parent = headers.get("parent_id", 2)
            c_label = headers.get("label", 3)
            c_type = headers.get("node_type", 4)
            c_tname = headers.get("template_name", 5)
            c_prefix = headers.get("node_id_prefix", 6)
            c_start = headers.get("start_index", 7)
            c_suffix = headers.get("label_suffix", 8)
            c_enabled = headers.get("enabled", 9)
            tree_nodes = []
            for r in range(2, ws_tree.max_row + 1):
                enabled_raw = str(ws_tree.cell(r, c_enabled).value or "1").strip().lower()
                if enabled_raw in {"0", "false", "no"}:
                    continue
                node_type = str(ws_tree.cell(r, c_type).value or "fixed").strip().lower() or "fixed"
                node_id = str(ws_tree.cell(r, c_node).value or "").strip()
                parent_id = str(ws_tree.cell(r, c_parent).value or "").strip()
                label = str(ws_tree.cell(r, c_label).value or "").strip()
                template_name = str(ws_tree.cell(r, c_tname).value or "").strip()
                prefix = str(ws_tree.cell(r, c_prefix).value or "").strip()
                suffix = str(ws_tree.cell(r, c_suffix).value or "").strip()
                try:
                    start_index = int(ws_tree.cell(r, c_start).value or 1)
                except Exception:
                    start_index = 1
                if node_type == "fixed":
                    if not node_id:
                        continue
                    if not _is_valid_text_cell(label):
                        label = str(default_fixed_by_id.get(node_id, {}).get("label", "")).strip()
                    if not _is_valid_text_cell(label):
                        continue
                    if not parent_id:
                        parent_id = str(default_fixed_by_id.get(node_id, {}).get("parent_id", "")).strip()
                    tree_nodes.append(
                        {"node_id": node_id, "parent_id": parent_id, "label": label, "node_type": "fixed"}
                    )
                elif node_type == "template":
                    if not template_name:
                        continue
                    if not prefix:
                        prefix = str(default_tpl_by_name.get(template_name, {}).get("node_id_prefix", "")).strip()
                    if not prefix:
                        continue
                    if not parent_id:
                        parent_id = str(default_tpl_by_name.get(template_name, {}).get("parent_id", "")).strip()
                    if not _is_valid_text_cell(suffix):
                        suffix = str(default_tpl_by_name.get(template_name, {}).get("label_suffix", "")).strip()
                    if not start_index or start_index < 1:
                        try:
                            start_index = int(default_tpl_by_name.get(template_name, {}).get("start_index", 1))
                        except Exception:
                            start_index = 1
                    tree_nodes.append(
                        {
                            "node_id": "",
                            "parent_id": parent_id,
                            "label": "",
                            "node_type": "template",
                            "template_name": template_name,
                            "node_id_prefix": prefix,
                            "start_index": start_index,
                            "label_suffix": suffix,
                        }
                    )
            if tree_nodes:
                cfg["tree_nodes"] = tree_nodes

        ws_tpl = get_sheet_by_loose_name(wb, ["text_templates", "文本模板"])
        if ws_tpl is not None:
            headers = {str(ws_tpl.cell(1, c).value or "").strip(): c for c in range(1, ws_tpl.max_column + 1)}
            c_scene = headers.get("scene", 2)
            c_tpl = headers.get("template_text_zh", 3)
            c_enabled = headers.get("enabled", 5)
            merged = dict(cfg.get("text_templates", {}))
            for r in range(2, ws_tpl.max_row + 1):
                enabled_raw = str(ws_tpl.cell(r, c_enabled).value or "1").strip().lower()
                if enabled_raw in {"0", "false", "no"}:
                    continue
                scene = str(ws_tpl.cell(r, c_scene).value or "").strip()
                tpl = str(ws_tpl.cell(r, c_tpl).value or "").strip()
                if not scene or not _is_valid_text_cell(tpl):
                    continue
                # Keep Chinese output style by default; skip placeholder English templates.
                if not re.search(r"[\u4e00-\u9fff]", tpl):
                    continue
                merged[scene] = tpl
            cfg["text_templates"] = merged
        break
    return cfg


def load_ratio_analysis_rules() -> Dict[str, Any]:
    cfg = {
        "tree_nodes": list(DEFAULT_RATIO_TREE),
        "catalog": {},
        "trend_threshold_pp": 2.0,
        "text_templates": {
            "indicator_value": DEFAULT_TEXT_TEMPLATES["ratio_indicator_value"],
            "indicator_trend": DEFAULT_TEXT_TEMPLATES["ratio_indicator_trend"],
        },
    }
    if not RATIO_RULEBOOK_PATH.exists():
        return cfg
    try:
        wb = load_workbook(RATIO_RULEBOOK_PATH, data_only=True)
    except Exception:
        return cfg

    ws_tree = get_sheet_by_loose_name(wb, ["indicator_tree"])
    if ws_tree is not None:
        headers = {str(ws_tree.cell(1, c).value or "").strip(): c for c in range(1, ws_tree.max_column + 1)}
        nodes = []
        for r in range(2, ws_tree.max_row + 1):
            enabled = str(ws_tree.cell(r, headers.get("enabled", 6)).value or "1").strip().lower()
            if enabled in {"0", "false", "no"}:
                continue
            nodes.append(
                {
                    "node_id": str(ws_tree.cell(r, headers.get("node_id", 1)).value or "").strip(),
                    "parent_id": str(ws_tree.cell(r, headers.get("parent_id", 2)).value or "").strip(),
                    "label_zh": str(ws_tree.cell(r, headers.get("label_zh", 3)).value or "").strip(),
                    "node_type": str(ws_tree.cell(r, headers.get("node_type", 4)).value or "indicator").strip(),
                    "indicator_id": str(ws_tree.cell(r, headers.get("indicator_id", 5)).value or "").strip(),
                    "sort_order": _safe_float(ws_tree.cell(r, headers.get("sort_order", 7)).value, 9999),
                }
            )
        nodes = [x for x in nodes if x.get("node_id") and x.get("label_zh")]
        if nodes:
            nodes.sort(key=lambda x: (x.get("sort_order", 9999), str(x.get("node_id"))))
            cfg["tree_nodes"] = nodes

    ws_cat = get_sheet_by_loose_name(wb, ["indicator_catalog"])
    if ws_cat is not None:
        headers = {str(ws_cat.cell(1, c).value or "").strip(): c for c in range(1, ws_cat.max_column + 1)}
        for r in range(2, ws_cat.max_row + 1):
            rid = str(ws_cat.cell(r, headers.get("indicator_id", 1)).value or "").strip()
            if not rid:
                continue
            enabled = str(ws_cat.cell(r, headers.get("enabled", 8)).value or "1").strip().lower()
            if enabled in {"0", "false", "no"}:
                continue
            cfg["catalog"][rid] = {
                "name": str(ws_cat.cell(r, headers.get("indicator_name_zh", 2)).value or rid).strip(),
                "direction": str(ws_cat.cell(r, headers.get("direction", 4)).value or "higher").strip().lower(),
                "unit": str(ws_cat.cell(r, headers.get("unit", 5)).value or "").strip(),
            }

    ws_display = get_sheet_by_loose_name(wb, ["display_policy"])
    if ws_display is not None:
        for r in range(2, ws_display.max_row + 1):
            key = str(ws_display.cell(r, 1).value or "").strip()
            if key == "trend_threshold_default_pp":
                cfg["trend_threshold_pp"] = _safe_float(ws_display.cell(r, 2).value, 2.0)
    ws_tpl = get_sheet_by_loose_name(wb, ["text_templates", "文本模板"])
    if ws_tpl is not None:
        headers = {str(ws_tpl.cell(1, c).value or "").strip(): c for c in range(1, ws_tpl.max_column + 1)}
        c_scene = headers.get("scene", 2)
        c_tpl = headers.get("template_text_zh", 3)
        c_enabled = headers.get("enabled", 5)
        merged = dict(cfg.get("text_templates", {}))
        for r in range(2, ws_tpl.max_row + 1):
            enabled_raw = str(ws_tpl.cell(r, c_enabled).value or "1").strip().lower()
            if enabled_raw in {"0", "false", "no"}:
                continue
            scene = str(ws_tpl.cell(r, c_scene).value or "").strip()
            tpl = str(ws_tpl.cell(r, c_tpl).value or "").strip()
            if not scene or not _is_valid_text_cell(tpl):
                continue
            merged[scene] = tpl
        cfg["text_templates"] = merged
    return cfg


def _is_key_ratio_topic(node: Dict[str, Any]) -> bool:
    """Only dedicated topic rows are considered key-ratio items."""
    ntype = str(node.get("node_type", "")).strip().lower()
    if ntype != "topic":
        return False
    iid = str(node.get("indicator_id", "")).strip().lower()
    return iid in {"topic_roe_dupont", "topic_gross_margin"}


def _sanitize_ratio_tree_nodes(tree_nodes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Guardrail for the regular ratio page:
    - keep group/indicator nodes only
    - remove any explicit key-topic rows
    """
    out: List[Dict[str, Any]] = []
    for n in tree_nodes:
        ntype = str(n.get("node_type", "")).strip().lower()
        if ntype not in {"group", "indicator"}:
            continue
        if _is_key_ratio_topic(n):
            continue
        out.append(n)
    return out


def load_key_ratio_text_rules() -> Dict[str, Any]:
    global _KEY_RATIO_RULES_CACHE
    if isinstance(_KEY_RATIO_RULES_CACHE, dict):
        return _KEY_RATIO_RULES_CACHE
    cfg: Dict[str, Any] = {
        "thresholds": {
            "roe": dict(DEFAULT_KEY_DRIVER_THRESHOLDS),
            "gross_margin": dict(DEFAULT_KEY_DRIVER_THRESHOLDS),
        },
        "templates": {
            "roe_headline": "{metric_name}由{y_prev}年的{v_prev}变动至{y_curr}年的{v_curr}，整体{trend_word}。",
            "roe_single_driver": "主要驱动因素为{driver_1}，贡献{contrib_1}个百分点，驱动占比{share_1}。",
            "roe_dual_driver": "变动由{driver_1}与{driver_2}共同驱动，分别贡献{contrib_1}/{contrib_2}个百分点，合计驱动占比{share_sum}。",
            "roe_multi_driver": "ROE变动为多因素共同作用：{driver_list}，需结合业务与资本结构综合判断。",
            "roe_offset": "存在显著对冲项：{offset_driver}（{offset_contrib}个百分点），抵消了{main_driver}的部分影响。",
            "gm_headline": "{metric_name}由{y_prev}年的{v_prev}变动至{y_curr}年的{v_curr}，整体{trend_word}。",
            "gm_single_driver": "主要驱动来自{driver_1}，贡献{contrib_1}个百分点，驱动占比{share_1}。",
            "gm_dual_driver": "变动由{driver_1}与{driver_2}共同驱动，分别贡献{contrib_1}/{contrib_2}个百分点，合计驱动占比{share_sum}。",
            "gm_multi_driver": "毛利率变动为多因素共同作用：{driver_list}。",
            "gm_structure_effect": "其中结构效应{structure_effect}个百分点，价格/成本效应{price_cost_effect}个百分点，交互项{interaction_effect}个百分点。",
            "gm_negative_profit_case": "在总毛利为负或分项正负并存情况下，采用影响占比口径：{impact_summary}。",
        },
    }
    if not KEY_RATIO_RULEBOOK_PATH.exists():
        _KEY_RATIO_RULES_CACHE = cfg
        return cfg
    try:
        wb = load_workbook(KEY_RATIO_RULEBOOK_PATH, data_only=True)
    except Exception:
        _KEY_RATIO_RULES_CACHE = cfg
        return cfg

    ws_th = get_sheet_by_loose_name(wb, ["driver_thresholds"])
    if ws_th is not None:
        headers = {str(ws_th.cell(1, c).value or "").strip(): c for c in range(1, ws_th.max_column + 1)}
        c_scope = headers.get("scope", 1)
        c_key = headers.get("threshold_key", 3)
        c_val = headers.get("threshold_value", 4)
        c_enabled = headers.get("enabled", 7)
        for r in range(2, ws_th.max_row + 1):
            enabled_raw = str(ws_th.cell(r, c_enabled).value or "1").strip().lower()
            if enabled_raw in {"0", "false", "no"}:
                continue
            scope = str(ws_th.cell(r, c_scope).value or "").strip().lower()
            key = str(ws_th.cell(r, c_key).value or "").strip()
            val = _safe_float(ws_th.cell(r, c_val).value, None)  # type: ignore[arg-type]
            if scope not in {"roe", "gross_margin"} or not key or val is None:
                continue
            cfg["thresholds"].setdefault(scope, dict(DEFAULT_KEY_DRIVER_THRESHOLDS))
            cfg["thresholds"][scope][key] = float(val)

    ws_tpl = get_sheet_by_loose_name(wb, ["narrative_templates", "文本模板"])
    if ws_tpl is not None:
        headers = {str(ws_tpl.cell(1, c).value or "").strip(): c for c in range(1, ws_tpl.max_column + 1)}
        c_id = headers.get("template_id", 2)
        c_tpl = headers.get("template_text", 4)
        c_enabled = headers.get("enabled", 6)
        merged = dict(cfg.get("templates", {}))
        for r in range(2, ws_tpl.max_row + 1):
            enabled_raw = str(ws_tpl.cell(r, c_enabled).value or "1").strip().lower()
            if enabled_raw in {"0", "false", "no"}:
                continue
            tid = str(ws_tpl.cell(r, c_id).value or "").strip()
            tpl = str(ws_tpl.cell(r, c_tpl).value or "").strip()
            if not tid or not _is_valid_text_cell(tpl):
                continue
            merged[tid] = tpl
        cfg["templates"] = merged
    _KEY_RATIO_RULES_CACHE = cfg
    return cfg


def _fmt_ratio_value(v: Optional[float], unit: str) -> str:
    if v is None:
        return "待补充"
    if unit == "%":
        vv = v * 100.0 if abs(v) <= 1.0 else v
        return f"{vv:.2f}%"
    return f"{v:.2f}{unit}" if unit else f"{v:.2f}"


def _fmt_ratio_number(v: Optional[float], unit: str) -> str:
    if v is None:
        return "待补充"
    if unit == "%":
        vv = v * 100.0 if abs(v) <= 1.0 else v
        return f"{vv:.2f}"
    return f"{v:.2f}"


def _fmt_ratio_trend(curr: Optional[float], prev: Optional[float], direction: str, threshold: float, unit: str) -> str:
    if curr is None or prev is None:
        return "待补充"
    c = curr
    p = prev
    if unit == "%":
        c = curr * 100.0 if abs(curr) <= 1.0 else curr
        p = prev * 100.0 if abs(prev) <= 1.0 else prev
    delta = c - p
    if abs(delta) <= threshold:
        return f"基本稳定（变动{abs(delta):.2f}{'个百分点' if unit == '%' else ''}）"
    improve = (delta > 0 and direction != "lower") or (delta < 0 and direction == "lower")
    verb = "改善" if improve else "弱化"
    return f"{verb}（变动{abs(delta):.2f}{'个百分点' if unit == '%' else ''}）"


def _ratio_trend_parts(curr: Optional[float], prev: Optional[float], direction: str, threshold: float, unit: str) -> Dict[str, str]:
    if curr is None or prev is None:
        return {"judgement": "待补充", "delta": "待补充", "unit2": "个百分点" if unit == "%" else (unit or "")}
    c = curr
    p = prev
    if unit == "%":
        c = curr * 100.0 if abs(curr) <= 1.0 else curr
        p = prev * 100.0 if abs(prev) <= 1.0 else prev
    delta = c - p
    if abs(delta) <= threshold:
        judgement = "基本稳定"
    else:
        improve = (delta > 0 and direction != "lower") or (delta < 0 and direction == "lower")
        judgement = "改善" if improve else "弱化"
    return {
        "judgement": judgement,
        "delta": f"{abs(delta):.2f}",
        "unit2": "个百分点" if unit == "%" else (unit or ""),
    }


def _pick_ratio_record(rows: List[Dict[str, Any]], code_keys: List[str], name_keys: List[str]) -> Optional[Dict[str, Any]]:
    for r in rows:
        code = str(r.get("code", "")).strip().lower()
        name = str(r.get("name", "")).strip()
        if any(k and k.lower() in code for k in code_keys):
            return r
        if any(k and k in name for k in name_keys):
            return r
    return None


def _classify_driver_case(contrib: List[tuple], thresholds: Dict[str, float]) -> Dict[str, Any]:
    usable = [(k, v) for k, v in contrib if v is not None]
    if not usable:
        return {"case": "none", "top": [], "shares": {}, "main_driver": "待补充"}
    ordered = sorted(usable, key=lambda x: abs(x[1]), reverse=True)
    total_abs = sum(abs(v) for _, v in ordered)
    shares = {k: (abs(v) / total_abs if total_abs else 0.0) for k, v in ordered}
    single = float(thresholds.get("single_driver_share", DEFAULT_KEY_DRIVER_THRESHOLDS["single_driver_share"]))
    dual_sum = float(thresholds.get("dual_driver_share_sum", DEFAULT_KEY_DRIVER_THRESHOLDS["dual_driver_share_sum"]))
    dual_each = float(thresholds.get("dual_driver_each_min", DEFAULT_KEY_DRIVER_THRESHOLDS["dual_driver_each_min"]))
    if shares.get(ordered[0][0], 0.0) >= single:
        case = "single"
    elif len(ordered) >= 2 and (shares.get(ordered[0][0], 0.0) + shares.get(ordered[1][0], 0.0) >= dual_sum) and (
        shares.get(ordered[0][0], 0.0) >= dual_each and shares.get(ordered[1][0], 0.0) >= dual_each
    ):
        case = "dual"
    else:
        case = "multi"
    return {"case": case, "top": ordered, "shares": shares, "main_driver": ordered[0][0]}


def _dupont_topic_text(rows: List[Dict[str, Any]], years: List[str], key_rules: Dict[str, Any]) -> str:
    if len(years) < 3:
        return "杜邦分析所需年度不足，待补充。"
    y1, y2, y3 = years[:3]
    tpl = key_rules.get("templates", {})
    th = key_rules.get("thresholds", {}).get("roe", DEFAULT_KEY_DRIVER_THRESHOLDS)
    rec_roe = _pick_ratio_record(rows, ["roe"], ["净资产收益率", "ROE"])
    rec_nm = _pick_ratio_record(rows, ["net_margin", "sales_net_margin"], ["销售净利率", "净利率"])
    rec_at = _pick_ratio_record(rows, ["asset_turnover"], ["总资产周转率"])
    rec_em = _pick_ratio_record(rows, ["equity_multiplier"], ["权益乘数"])

    def gv(rec, y):
        if not rec:
            return None
        vals = rec.get("values", {}) if isinstance(rec.get("values"), dict) else {}
        return vals.get(y)

    def norm_pct(v):
        if v is None:
            return None
        return v * 100.0 if abs(v) <= 1.0 else v

    roe = {y: norm_pct(gv(rec_roe, y)) for y in years[:3]}
    nm = {y: norm_pct(gv(rec_nm, y)) for y in years[:3]}
    at = {y: gv(rec_at, y) for y in years[:3]}
    em = {y: gv(rec_em, y) for y in years[:3]}
    for y in years[:3]:
        if em.get(y) is None:
            n = nm.get(y)
            a = at.get(y)
            r = roe.get(y)
            if n not in (None, 0) and a not in (None, 0) and r is not None:
                em[y] = (r / 100.0) / ((n / 100.0) * a)

    d_roe = None if roe.get(y3) is None or roe.get(y1) is None else roe.get(y3) - roe.get(y1)
    d_nm = None if nm.get(y3) is None or nm.get(y1) is None else nm.get(y3) - nm.get(y1)
    d_at = None if at.get(y3) is None or at.get(y1) is None else at.get(y3) - at.get(y1)
    d_em = None if em.get(y3) is None or em.get(y1) is None else em.get(y3) - em.get(y1)
    contrib = [("净利率", d_nm), ("总资产周转率", d_at), ("权益乘数", d_em)]
    c = _classify_driver_case(contrib, th)
    top = c["top"]
    shares = c["shares"]
    main_driver = c["main_driver"]
    headline = _render_template(
        tpl.get("roe_headline", "{metric_name}由{y_prev}年的{v_prev}变动至{y_curr}年的{v_curr}，整体{trend_word}。"),
        {
            "metric_name": "ROE",
            "y_prev": y1,
            "v_prev": _fmt_ratio_value(roe.get(y1), "%"),
            "y_curr": y3,
            "v_curr": _fmt_ratio_value(roe.get(y3), "%"),
            "trend_word": _trend_word(roe.get(y3), roe.get(y1), stable_pp=float(th.get("delta_stable_pp", 2.0))),
        },
    )
    if c["case"] == "single":
        driver_txt = _render_template(
            tpl.get("roe_single_driver", "主要驱动因素为{driver_1}，贡献{contrib_1}个百分点，驱动占比{share_1}。"),
            {
                "driver_1": top[0][0],
                "contrib_1": f"{abs(top[0][1]):.2f}",
                "share_1": f"{shares.get(top[0][0], 0.0) * 100:.2f}%",
            },
        )
    elif c["case"] == "dual":
        driver_txt = _render_template(
            tpl.get("roe_dual_driver", "变动由{driver_1}与{driver_2}共同驱动，分别贡献{contrib_1}/{contrib_2}个百分点，合计驱动占比{share_sum}。"),
            {
                "driver_1": top[0][0],
                "driver_2": top[1][0],
                "contrib_1": f"{abs(top[0][1]):.2f}",
                "contrib_2": f"{abs(top[1][1]):.2f}",
                "share_sum": f"{(shares.get(top[0][0], 0.0) + shares.get(top[1][0], 0.0)) * 100:.2f}%",
            },
        )
    else:
        driver_txt = _render_template(
            tpl.get("roe_multi_driver", "ROE变动为多因素共同作用：{driver_list}，需结合业务与资本结构综合判断。"),
            {"driver_list": "、".join([k for k, _ in top])},
        )

    offset_txt = ""
    if d_roe is not None:
        sig = float(th.get("significant_abs_contrib", DEFAULT_KEY_DRIVER_THRESHOLDS["significant_abs_contrib"]))
        opp = [(k, v) for k, v in top if (v * d_roe) < 0 and abs(v) >= sig]
        if opp:
            offset_txt = _render_template(
                tpl.get("roe_offset", "存在显著对冲项：{offset_driver}（{offset_contrib}个百分点），抵消了{main_driver}的部分影响。"),
                {
                    "offset_driver": opp[0][0],
                    "offset_contrib": f"{abs(opp[0][1]):.2f}",
                    "main_driver": main_driver,
                },
            )
    factors_txt = _render_template(
        tpl.get("key_roe_factors", DEFAULT_TEXT_TEMPLATES["key_roe_factors"]),
        {
            "y1": y1,
            "y2": y2,
            "y3": y3,
            "nm1": _fmt_ratio_value(nm.get(y1), "%"),
            "nm2": _fmt_ratio_value(nm.get(y2), "%"),
            "nm3": _fmt_ratio_value(nm.get(y3), "%"),
            "at1": _fmt_ratio_value(at.get(y1), "x"),
            "at2": _fmt_ratio_value(at.get(y2), "x"),
            "at3": _fmt_ratio_value(at.get(y3), "x"),
            "em1": _fmt_ratio_value(em.get(y1), "x"),
            "em2": _fmt_ratio_value(em.get(y2), "x"),
            "em3": _fmt_ratio_value(em.get(y3), "x"),
        },
    )
    return _append_text(_append_text(headline, driver_txt), _append_text(factors_txt, offset_txt))


def _gross_margin_topic_text(wb, rows: List[Dict[str, Any]], years: List[str], key_rules: Dict[str, Any]) -> str:
    if len(years) < 3:
        return "毛利率贡献分析所需年度不足，待补充。"
    y1, y2, y3 = years[:3]
    tpl = key_rules.get("templates", {})
    th = key_rules.get("thresholds", {}).get("gross_margin", DEFAULT_KEY_DRIVER_THRESHOLDS)
    segs = _detect_income_segments(wb, years)
    if not segs:
        return "未识别到营业收入分项，毛利率贡献待补充。"

    rec_gm = _pick_ratio_record(rows, ["gross_margin"], ["销售毛利率", "毛利率"])
    gm = {}
    for y in years[:3]:
        v = None
        if rec_gm and isinstance(rec_gm.get("values"), dict):
            v = rec_gm["values"].get(y)
        gm[y] = v * 100.0 if (v is not None and abs(v) <= 1.0) else v

    rev_total = {}
    gp_total = {}
    for y in years[:3]:
        revs = [(s.get("revenue_values", {}) or {}).get(y) for s in segs]
        gps = [(s.get("gross_values", {}) or {}).get(y) for s in segs]
        rev_total[y] = sum([v for v in revs if v is not None]) if any(v is not None for v in revs) else None
        gp_total[y] = sum([v for v in gps if v is not None]) if any(v is not None for v in gps) else None

    items = []
    for s in segs:
        name = str(s.get("label", ""))
        rv = (s.get("revenue_values", {}) or {}).get(y3)
        gv = (s.get("gross_values", {}) or {}).get(y3)
        share = None if rv is None or rev_total.get(y3) in (None, 0) else rv / rev_total[y3] * 100.0
        margin = None if rv in (None, 0) or gv is None else gv / rv * 100.0
        items.append((name, share, margin))
    items = sorted(items, key=lambda x: abs(x[1]) if x[1] is not None else -1, reverse=True)[:3]
    top_txt = "；".join([f"{n} 收入占比{_fmt_ratio_value(s, '%')}、分项毛利率{_fmt_ratio_value(m, '%')}" for n, s, m in items]) if items else "待补充"

    # Gross margin driver decomposition: structure + margin + interaction.
    seg_names = sorted({str(s.get("label", "")) for s in segs if str(s.get("label", "")).strip()})
    structure_effect = 0.0
    margin_effect = 0.0
    interaction_effect = 0.0
    has_component = False
    for name in seg_names:
        seg = next((x for x in segs if str(x.get("label", "")) == name), None)
        if not seg:
            continue
        r1 = (seg.get("revenue_values", {}) or {}).get(y1)
        r3 = (seg.get("revenue_values", {}) or {}).get(y3)
        g1 = (seg.get("gross_values", {}) or {}).get(y1)
        g3 = (seg.get("gross_values", {}) or {}).get(y3)
        if rev_total.get(y1) in (None, 0) or rev_total.get(y3) in (None, 0) or r1 is None or r3 is None:
            continue
        s1 = r1 / rev_total[y1]
        s3 = r3 / rev_total[y3]
        m1 = None if r1 in (None, 0) or g1 is None else g1 / r1 * 100.0
        m3 = None if r3 in (None, 0) or g3 is None else g3 / r3 * 100.0
        if m1 is None or m3 is None:
            continue
        has_component = True
        structure_effect += (s3 - s1) * m1
        margin_effect += s1 * (m3 - m1)
        interaction_effect += (s3 - s1) * (m3 - m1)

    contrib = [("结构效应", structure_effect if has_component else None), ("价格/成本效应", margin_effect if has_component else None), ("交互效应", interaction_effect if has_component else None)]
    c = _classify_driver_case(contrib, th)
    top = c["top"]
    shares = c["shares"]

    headline = _render_template(
        tpl.get("gm_headline", "{metric_name}由{y_prev}年的{v_prev}变动至{y_curr}年的{v_curr}，整体{trend_word}。"),
        {
            "metric_name": "毛利率",
            "y_prev": y1,
            "v_prev": _fmt_ratio_value(gm.get(y1), "%"),
            "y_curr": y3,
            "v_curr": _fmt_ratio_value(gm.get(y3), "%"),
            "trend_word": _trend_word(gm.get(y3), gm.get(y1), stable_pp=float(th.get("delta_stable_pp", 2.0))),
        },
    )
    if c["case"] == "single" and top:
        driver_txt = _render_template(
            tpl.get("gm_single_driver", "主要驱动来自{driver_1}，贡献{contrib_1}个百分点，驱动占比{share_1}。"),
            {"driver_1": top[0][0], "contrib_1": f"{abs(top[0][1]):.2f}", "share_1": f"{shares.get(top[0][0], 0.0) * 100:.2f}%"},
        )
    elif c["case"] == "dual" and len(top) >= 2:
        driver_txt = _render_template(
            tpl.get("gm_dual_driver", "变动由{driver_1}与{driver_2}共同驱动，分别贡献{contrib_1}/{contrib_2}个百分点，合计驱动占比{share_sum}。"),
            {
                "driver_1": top[0][0],
                "driver_2": top[1][0],
                "contrib_1": f"{abs(top[0][1]):.2f}",
                "contrib_2": f"{abs(top[1][1]):.2f}",
                "share_sum": f"{(shares.get(top[0][0], 0.0) + shares.get(top[1][0], 0.0)) * 100:.2f}%",
            },
        )
    else:
        driver_txt = _render_template(
            tpl.get("gm_multi_driver", "毛利率变动为多因素共同作用：{driver_list}。"),
            {"driver_list": "、".join([k for k, _ in top]) if top else "待补充"},
        )
    effect_txt = _render_template(
        tpl.get("gm_structure_effect", "其中结构效应{structure_effect}个百分点，价格/成本效应{price_cost_effect}个百分点，交互项{interaction_effect}个百分点。"),
        {
            "structure_effect": f"{abs(structure_effect):.2f}",
            "price_cost_effect": f"{abs(margin_effect):.2f}",
            "interaction_effect": f"{abs(interaction_effect):.2f}",
        },
    )
    top_seg_txt = _render_template(
        tpl.get("key_gm_top_segments", DEFAULT_TEXT_TEMPLATES["key_gm_top_segments"]),
        {"top_txt": top_txt},
    )
    extra = ""
    mixed_sign = any((x.get("gross_values", {}) or {}).get(y3, 0) > 0 for x in segs) and any((x.get("gross_values", {}) or {}).get(y3, 0) < 0 for x in segs)
    if gp_total.get(y3) is not None and gp_total.get(y3) < 0 or mixed_sign:
        extra = _render_template(
            tpl.get("gm_negative_profit_case", "在总毛利为负或分项正负并存情况下，采用影响占比口径：{impact_summary}。"),
            {"impact_summary": "各分项按绝对影响占比展示，不采用简单占比"},
        )
    return _append_text(_append_text(headline, driver_txt), _append_text(_append_text(effect_txt, top_seg_txt), extra))


def build_ratio_analysis_map(wb, project_id: str) -> Dict[str, Any]:
    ws = get_sheet_by_loose_name(wb, ["财务比率", "财务指标", "财务指标表"])
    if ws is None:
        return {"sheet_title": None, "years": [], "tree": [], "nodes": []}

    ratio_data = read_ratio_rows(ws)
    years = ratio_data.get("years", [])[:3]
    rows = ratio_data.get("rows", [])
    ratio_by_code = {str(r.get("code", "")).strip(): r for r in rows}
    ratio_by_name = {str(r.get("name", "")).strip(): r for r in rows}

    rules = load_ratio_analysis_rules()
    tree_rules = _sanitize_ratio_tree_nodes(rules.get("tree_nodes", []))
    catalog = rules.get("catalog", {})
    threshold = float(rules.get("trend_threshold_pp", 2.0))
    text_templates = dict(rules.get("text_templates", {}))

    tree = []
    nodes = []

    def add_node(node_id: str, parent_id: str, label: str, values: Dict[str, Optional[float]], source_code: str, source_name: str, auto_text: str):
        nodes.append(
            {
                "node_id": node_id,
                "parent_id": parent_id,
                "label": label,
                "source_code": source_code,
                "source_name": source_name,
                "values": values,
                "auto_text": auto_text,
            }
        )

    for t in tree_rules:
        nid = str(t.get("node_id", "")).strip()
        parent = str(t.get("parent_id", "")).strip()
        label = str(t.get("label_zh", "")).strip()
        ntype = str(t.get("node_type", "group")).strip().lower()
        iid = str(t.get("indicator_id", "")).strip()
        if not nid or not label:
            continue
        # Extra hard guard: regular page never accepts key page node IDs.
        if nid in KEY_RATIO_IDS:
            continue
        tree.append({"node_id": nid, "parent_id": parent, "label": label})
        if ntype != "indicator":
            continue
        cat = catalog.get(iid, {"name": label, "direction": "higher", "unit": "%"})
        rec = ratio_by_code.get(iid) or ratio_by_name.get(cat.get("name", "")) or {}
        vals = rec.get("values", {}) if isinstance(rec.get("values"), dict) else {}
        val_map = {y: vals.get(y) for y in years}
        y1, y2, y3 = (years + ["", "", ""])[:3]
        unit = cat.get("unit", "%")
        p21 = _ratio_trend_parts(val_map.get(y2), val_map.get(y1), cat.get("direction", "higher"), threshold, unit)
        p32 = _ratio_trend_parts(val_map.get(y3), val_map.get(y2), cat.get("direction", "higher"), threshold, unit)
        value_tpl = text_templates.get("indicator_value", DEFAULT_TEXT_TEMPLATES["ratio_indicator_value"])
        trend_tpl = text_templates.get("indicator_trend", DEFAULT_TEXT_TEMPLATES["ratio_indicator_trend"])
        text1 = _render_template(
            value_tpl,
            {
                "name": label,
                "unit": unit,
                "y1": y1,
                "y2": y2,
                "y3": y3,
                "v1": _fmt_ratio_number(val_map.get(y1), unit),
                "v2": _fmt_ratio_number(val_map.get(y2), unit),
                "v3": _fmt_ratio_number(val_map.get(y3), unit),
            },
        )
        text2 = _render_template(
            trend_tpl,
            {
                "y1": y1,
                "y2": y2,
                "y3": y3,
                "judgement21": p21["judgement"],
                "judgement32": p32["judgement"],
                "delta21": p21["delta"],
                "delta32": p32["delta"],
                "unit2": p21["unit2"],
            },
        )
        auto_text = _append_text(text1, text2)
        add_node(nid, parent, label, val_map, str(rec.get("code", "")), str(rec.get("name", "")), auto_text)

    store = load_store(project_id)
    entries = store.get("entries", {})
    for n in nodes:
        saved = entries.get(make_entry_key("ratio_analysis", n["node_id"]), {})
        manual = str(saved.get("manual_text", "") or "")
        confirmed = bool(saved.get("confirmed", False))
        n["manual_text"] = manual
        n["confirmed"] = confirmed
        n["final_text"] = manual if manual.strip() else n["auto_text"]

    return {"sheet_title": ratio_data.get("sheet_title"), "years": years, "tree": tree, "nodes": nodes}


def build_key_ratio_analysis_map(wb, project_id: str) -> Dict[str, Any]:
    ws = get_sheet_by_loose_name(wb, ["财务比率", "财务指标", "财务指标表"])
    if ws is None:
        return {"sheet_title": None, "years": [], "tree": [], "nodes": []}

    ratio_data = read_ratio_rows(ws)
    years = ratio_data.get("years", [])[:3]
    rows = ratio_data.get("rows", [])

    tree = [
        {"node_id": "K1", "parent_id": "", "label": "ROE重点分析"},
        {"node_id": "K2", "parent_id": "", "label": "毛利率重点分析"},
    ]

    key_rules = load_key_ratio_text_rules()
    roe_text = _dupont_topic_text(rows, years, key_rules)
    gm_text = _gross_margin_topic_text(wb, rows, years, key_rules)
    nodes = [
        {
            "node_id": "K1",
            "parent_id": "",
            "label": "ROE重点分析",
            "source_code": "roe",
            "source_name": "净资产收益率",
            "values": {y: None for y in years},
            "auto_text": roe_text,
        },
        {
            "node_id": "K2",
            "parent_id": "",
            "label": "毛利率重点分析",
            "source_code": "gross_margin",
            "source_name": "销售毛利率",
            "values": {y: None for y in years},
            "auto_text": gm_text,
        },
    ]
    # Hard guard: key page only renders dedicated K1/K2 nodes.
    tree = [t for t in tree if str(t.get("node_id", "")).strip() in KEY_RATIO_IDS]
    nodes = [n for n in nodes if str(n.get("node_id", "")).strip() in KEY_RATIO_IDS]

    store = load_store(project_id)
    entries = store.get("entries", {})
    for n in nodes:
        saved = entries.get(make_entry_key("key_ratio_analysis", n["node_id"]), {})
        manual = str(saved.get("manual_text", "") or "")
        confirmed = bool(saved.get("confirmed", False))
        n["manual_text"] = manual
        n["confirmed"] = confirmed
        n["final_text"] = manual if manual.strip() else n["auto_text"]

    return {"sheet_title": ratio_data.get("sheet_title"), "years": years, "tree": tree, "nodes": nodes}


def read_ratio_rows(ws) -> Dict[str, Any]:
    """Read ratio sheet in long format and pivot to wide by year."""
    # Default column positions by current template.
    idx_col, name_col, period_col, value_col = 1, 2, 4, 5

    # Try detect columns by header text in row 1.
    headers = {c: str(ws.cell(1, c).value or "").strip() for c in range(1, ws.max_column + 1)}
    for c, h in headers.items():
        if h == "指标ID":
            idx_col = c
        elif h == "指标":
            name_col = c
        elif h == "期间":
            period_col = c
        elif h == "数值":
            value_col = c

    years_set = set()
    bucket: Dict[str, Dict[str, Any]] = {}

    for r in range(2, ws.max_row + 1):
        code = str(ws.cell(r, idx_col).value or "").strip()
        name = str(ws.cell(r, name_col).value or "").strip()
        period_text = str(ws.cell(r, period_col).value or "").strip()
        value = normalize_num(ws.cell(r, value_col).value)
        if not code or not name:
            continue

        m = re.search(r"(20\d{2})", period_text)
        if not m:
            continue
        year = m.group(1)
        years_set.add(year)

        obj = bucket.setdefault(code, {"code": code, "name": name, "values": {}})
        obj["name"] = name
        obj["values"][year] = value

    years = sorted(years_set)[:DEFAULT_YEAR_COUNT]
    rows = list(bucket.values())
    for row in rows:
        for y in years:
            row["values"].setdefault(y, None)

    return {"sheet_title": ws.title, "years": years, "rows": rows}


def read_sheet_rows(wb, group: Dict[str, Any]) -> Dict[str, Any]:
    ws = get_sheet_by_loose_name(wb, group["candidates"])
    if ws is None:
        return {"sheet_title": None, "years": [], "rows": []}

    if group["id"] == "ratio":
        return read_ratio_rows(ws)

    years = parse_years_from_sheet(ws)
    rows: List[Dict[str, Any]] = []

    for r in range(2, ws.max_row + 1):
        code = str(ws.cell(r, 1).value or "").strip()
        name = str(ws.cell(r, 2).value or "").strip()
        if not code or not name:
            continue

        if group["id"] in {"bs", "is", "cf"} and not re.match(r"^(BS|IS|CF)\d+$", code):
            continue

        vals: Dict[str, Optional[float]] = {}
        for i, year in enumerate(years):
            vals[year] = normalize_num(ws.cell(r, 3 + i).value)

        rows.append({"code": code, "name": name, "values": vals})

    return {"sheet_title": ws.title, "years": years, "rows": rows}


def trend_text(curr: Optional[float], prev: Optional[float], stable_pct: float = 2.0) -> str:
    if curr is None or prev in (None, 0):
        return "待补充"
    pct = (curr - prev) / abs(prev) * 100.0
    if pct > stable_pct:
        return f"增加{abs(pct):.2f}%"
    if pct < -stable_pct:
        return f"减少{abs(pct):.2f}%"
    return f"保持稳定（变动{abs(pct):.2f}%）"


def _empty_year_values(years: List[str]) -> Dict[str, Optional[float]]:
    return {y: None for y in years[:3]}


def _sum_values(items: List[Dict[str, Optional[float]]], years: List[str]) -> Dict[str, Optional[float]]:
    out = _empty_year_values(years)
    for y in out.keys():
        vals = [x.get(y) for x in items if x.get(y) is not None]
        out[y] = sum(vals) if vals else None
    return out


def _fmt_income_auto_text(
    label: str,
    values: Dict[str, Optional[float]],
    years: List[str],
    stable_pct: float = 2.0,
    text_templates: Optional[Dict[str, str]] = None,
) -> str:
    if len(years) < 3:
        return ""
    y1, y2, y3 = years[:3]
    t21 = trend_text(values.get(y2), values.get(y1), stable_pct=stable_pct)
    t32 = trend_text(values.get(y3), values.get(y2), stable_pct=stable_pct)
    tpl = (text_templates or {}).get("income_segment_value", DEFAULT_TEXT_TEMPLATES["income_segment_value"])
    return _render_template(
        tpl,
        {
            "label": label,
            "y1": y1,
            "y2": y2,
            "y3": y3,
            "v1": values.get(y1) if values.get(y1) is not None else "待补充",
            "v2": values.get(y2) if values.get(y2) is not None else "待补充",
            "v3": values.get(y3) if values.get(y3) is not None else "待补充",
            "t21": t21,
            "t32": t32,
        },
    )


def _fmt_pct(v: Optional[float]) -> str:
    return f"{v:.2f}%" if v is not None else "待补充"


def _calc_ratio_pct(numer: Optional[float], denom: Optional[float]) -> Optional[float]:
    if numer is None or denom in (None, 0):
        return None
    return numer / denom * 100.0


def _trend_pp_text(curr: Optional[float], prev: Optional[float], stable_pp: float = 2.0) -> str:
    if curr is None or prev is None:
        return "待补充"
    delta = curr - prev
    if delta > stable_pp:
        return f"上升{abs(delta):.2f}个百分点"
    if delta < -stable_pp:
        return f"下降{abs(delta):.2f}个百分点"
    return f"基本稳定（变动{abs(delta):.2f}个百分点）"


def _trend_word(curr: Optional[float], prev: Optional[float], stable_pp: float = 2.0) -> str:
    if curr is None or prev is None:
        return "待补充"
    delta = curr - prev
    if delta > stable_pp:
        return "上升"
    if delta < -stable_pp:
        return "下降"
    return "基本稳定"


def _fmt_ratio_auto_text(
    ratio_label: str,
    numer_values: Dict[str, Optional[float]],
    denom_values: Dict[str, Optional[float]],
    years: List[str],
    stable_pp: float = 2.0,
    text_templates: Optional[Dict[str, str]] = None,
) -> str:
    if len(years) < 3:
        return ""
    y1, y2, y3 = years[:3]
    r1 = _calc_ratio_pct(numer_values.get(y1), denom_values.get(y1))
    r2 = _calc_ratio_pct(numer_values.get(y2), denom_values.get(y2))
    r3 = _calc_ratio_pct(numer_values.get(y3), denom_values.get(y3))
    t21 = _trend_pp_text(r2, r1, stable_pp=stable_pp)
    t32 = _trend_pp_text(r3, r2, stable_pp=stable_pp)
    tpl = (text_templates or {}).get("income_segment_share", DEFAULT_TEXT_TEMPLATES["income_segment_share"])
    return _render_template(
        tpl,
        {
            "ratio_label": ratio_label,
            "y1": y1,
            "y2": y2,
            "y3": y3,
            "r1": _fmt_pct(r1),
            "r2": _fmt_pct(r2),
            "r3": _fmt_pct(r3),
            "rt21": t21,
            "rt32": t32,
            "t21": t21,
            "t32": t32,
            "i1": _fmt_pct(r1),
            "i2": _fmt_pct(r2),
            "i3": _fmt_pct(r3),
            "it21": t21,
            "it32": t32,
        },
    )


def _append_text(base: str, addon: str) -> str:
    b = str(base or "").strip()
    a = str(addon or "").strip()
    if not b:
        return a
    if not a:
        return b
    return f"{b}\n{a}"


def _eval_income_formula(
    formula: str,
    by_node: Dict[str, Dict[str, Any]],
    years: List[str],
) -> Dict[str, Optional[float]]:
    expr = str(formula or "").strip()
    out = _empty_year_values(years)
    if not expr:
        return out
    m_sum = re.match(r"^SUM\((.+)\)$", expr, re.IGNORECASE)
    if m_sum:
        ids = [x.strip() for x in m_sum.group(1).split(",") if x.strip()]
        items = [by_node.get(i, {}).get("values", _empty_year_values(years)) for i in ids if i in by_node]
        return _sum_values(items, years)
    m_sub = re.match(r"^SUB\(([^,]+),([^)]+)\)$", expr, re.IGNORECASE)
    if m_sub:
        left_id, right_id = m_sub.group(1).strip(), m_sub.group(2).strip()
        lv = by_node.get(left_id, {}).get("values", _empty_year_values(years))
        rv = by_node.get(right_id, {}).get("values", _empty_year_values(years))
        for y in out.keys():
            lval = lv.get(y)
            rval = rv.get(y)
            if lval is None and rval is None:
                out[y] = None
            else:
                out[y] = (lval or 0.0) - (rval or 0.0)
        return out
    m_ref = re.match(r"^REF\((.+)\)$", expr, re.IGNORECASE)
    if m_ref:
        rid = m_ref.group(1).strip()
        return dict(by_node.get(rid, {}).get("values", _empty_year_values(years)))
    return out


def _pick_is_row(rows: List[Dict[str, Any]], code_candidates: List[str], name_keywords: List[str]) -> Optional[Dict[str, Any]]:
    by_code = {x["code"]: x for x in rows}
    for c in code_candidates:
        if c in by_code:
            return by_code[c]
    candidates: List[Dict[str, Any]] = []
    for r in rows:
        n = str(r.get("name", ""))
        if any(k in n for k in name_keywords):
            candidates.append(r)
    if candidates:
        # Prefer top-level item over "其中：..." sub-items when multiple names match.
        candidates.sort(key=lambda x: (1 if "其中" in str(x.get("name", "")) else 0, len(str(x.get("name", "")))))
        return candidates[0]
    return None


def _find_col_by_any(headers: Dict[int, str], needles: List[str]) -> Optional[int]:
    for c, h in headers.items():
        txt = str(h or "").strip()
        if not txt:
            continue
        if any(n in txt for n in needles):
            return c
    return None


def _detect_income_segments(wb, years: List[str]) -> List[Dict[str, Any]]:
    # Try to detect real segments from detail sheet; fallback to A/B/C placeholders.
    ws = get_sheet_by_loose_name(wb, ["明细_营业总收入", "明细_营业收入"])
    if ws is None or len(years) < 3:
        return [
            {"segment_id": "SEG_A", "label": "A项目（占位）", "revenue_values": _empty_year_values(years), "gross_values": _empty_year_values(years)},
            {"segment_id": "SEG_B", "label": "B项目（占位）", "revenue_values": _empty_year_values(years), "gross_values": _empty_year_values(years)},
            {"segment_id": "SEG_C", "label": "C项目（占位）", "revenue_values": _empty_year_values(years), "gross_values": _empty_year_values(years)},
        ]

    headers = {c: str(ws.cell(1, c).value or "").strip() for c in range(1, ws.max_column + 1)}
    year_col = _find_col_by_any(headers, ["期间", "年度"])
    seg_col = _find_col_by_any(headers, ["分项收入/子项名称", "分项收入/子项收入", "分项名称", "子项名称"])
    rev_col = _find_col_by_any(headers, ["分项收入明细值", "收入明细值", "明细值(万元)"])
    cost_col = _find_col_by_any(headers, ["分项成本明细值", "成本明细值"])
    gross_col = _find_col_by_any(headers, ["分项毛利明细值", "毛利明细值"])

    # Backward compatibility fallback by historical column positions.
    if year_col is None:
        year_col = 4
    if seg_col is None:
        seg_col = 5
    if rev_col is None:
        rev_col = 7

    revenue_bucket: Dict[str, Dict[str, Optional[float]]] = {}
    cost_bucket: Dict[str, Dict[str, Optional[float]]] = {}
    gross_bucket: Dict[str, Dict[str, Optional[float]]] = {}
    yset = set(years[:3])
    for r in range(2, ws.max_row + 1):
        year_text = str(ws.cell(r, year_col).value or "").strip()
        seg_name = str(ws.cell(r, seg_col).value or "").strip()
        rev_amount = normalize_num(ws.cell(r, rev_col).value)
        cost_amount = normalize_num(ws.cell(r, cost_col).value) if cost_col else None
        gross_amount = normalize_num(ws.cell(r, gross_col).value) if gross_col else None
        m = re.search(r"(20\d{2})", year_text)
        if not seg_name or not m:
            continue
        y = m.group(1)
        if y not in yset:
            continue
        r_obj = revenue_bucket.setdefault(seg_name, _empty_year_values(years))
        c_obj = cost_bucket.setdefault(seg_name, _empty_year_values(years))
        g_obj = gross_bucket.setdefault(seg_name, _empty_year_values(years))

        if rev_amount is not None:
            r_obj[y] = (r_obj.get(y) or 0.0) + rev_amount
        if cost_amount is not None:
            c_obj[y] = (c_obj.get(y) or 0.0) + cost_amount
        if gross_amount is not None:
            g_obj[y] = (g_obj.get(y) or 0.0) + gross_amount

    all_seg_names = set(revenue_bucket.keys()) | set(cost_bucket.keys()) | set(gross_bucket.keys())
    names = []
    for n in sorted(all_seg_names):
        rv = revenue_bucket.get(n, {})
        cv = cost_bucket.get(n, {})
        gv = gross_bucket.get(n, {})
        if any(v is not None for v in rv.values()) or any(v is not None for v in cv.values()) or any(
            v is not None for v in gv.values()
        ):
            names.append(n)
    if not names:
        return [
            {"segment_id": "SEG_A", "label": "A项目（占位）", "revenue_values": _empty_year_values(years), "gross_values": _empty_year_values(years)},
            {"segment_id": "SEG_B", "label": "B项目（占位）", "revenue_values": _empty_year_values(years), "gross_values": _empty_year_values(years)},
            {"segment_id": "SEG_C", "label": "C项目（占位）", "revenue_values": _empty_year_values(years), "gross_values": _empty_year_values(years)},
        ]

    out = []
    for i, name in enumerate(sorted(names), start=1):
        sid = f"SEG_{i}"
        rev_vals = revenue_bucket.get(name, _empty_year_values(years))
        cost_vals = cost_bucket.get(name, _empty_year_values(years))
        gross_vals = gross_bucket.get(name, _empty_year_values(years))
        # If gross detail is absent, derive by revenue - cost when possible.
        for y in years[:3]:
            if gross_vals.get(y) is None and rev_vals.get(y) is not None and cost_vals.get(y) is not None:
                gross_vals[y] = (rev_vals.get(y) or 0.0) - (cost_vals.get(y) or 0.0)
        out.append(
            {
                "segment_id": sid,
                "label": name,
                "revenue_values": rev_vals,
                "cost_values": cost_vals,
                "gross_values": gross_vals,
            }
        )
    return out


def build_income_analysis_map(wb, project_id: str) -> Dict[str, Any]:
    ws = get_sheet_by_loose_name(wb, ["利润表", "利润表 "])
    if ws is None:
        return {"sheet_title": None, "years": [], "tree": [], "nodes": []}

    years = parse_years_from_sheet(ws)
    years = years[:3]
    if len(years) < 3:
        return {"sheet_title": ws.title, "years": years, "tree": [], "nodes": []}
    y1, y2, y3 = years
    income_rules = load_income_rules()
    yoy_stable_pct = float(income_rules.get("yoy_stable_pct", 2.0))
    share_stable_pp = float(income_rules.get("share_stable_pp", 2.0))
    income_text_templates = dict(income_rules.get("text_templates", {}))

    is_rows: List[Dict[str, Any]] = []
    for r in range(2, ws.max_row + 1):
        code = str(ws.cell(r, 1).value or "").strip()
        name = str(ws.cell(r, 2).value or "").strip()
        if not code or not name or not re.match(r"^IS\d+$", code):
            continue
        is_rows.append(
            {
                "code": code,
                "name": name,
                "values": {y1: normalize_num(ws.cell(r, 3).value), y2: normalize_num(ws.cell(r, 4).value), y3: normalize_num(ws.cell(r, 5).value)},
            }
        )

    segments = _detect_income_segments(wb, years)

    def make_node(node_id: str, parent_id: str, label: str, values: Dict[str, Optional[float]], source_code: str = "", source_name: str = "") -> Dict[str, Any]:
        auto_text = _fmt_income_auto_text(
            label,
            values,
            years,
            stable_pct=yoy_stable_pct,
            text_templates=income_text_templates,
        )
        return {
            "node_id": node_id,
            "parent_id": parent_id,
            "label": label,
            "source_code": source_code,
            "source_name": source_name,
            "values": values,
            "auto_text": auto_text,
        }

    nodes: List[Dict[str, Any]] = []
    tree: List[Dict[str, str]] = []
    tree_rules = income_rules.get("tree_nodes", DEFAULT_INCOME_TREE)
    fixed_tree = [x for x in tree_rules if str(x.get("node_type", "fixed")).strip().lower() == "fixed"]
    template_tree = [x for x in tree_rules if str(x.get("node_type", "")).strip().lower() == "template"]
    fixed_label_map = {str(x.get("node_id", "")).strip(): str(x.get("label", "")).strip() for x in fixed_tree}
    fixed_parent_map = {str(x.get("node_id", "")).strip(): str(x.get("parent_id", "")).strip() for x in fixed_tree}

    def add_tree(node_id: str, parent_id: str, label: str) -> None:
        tree.append({"node_id": node_id, "parent_id": parent_id, "label": label})

    rev_tpl = next((x for x in template_tree if str(x.get("template_name", "")).strip() == "revenue_segment"), None)
    rev_prefix = str((rev_tpl or {}).get("node_id_prefix", "1.")).strip() or "1."
    rev_parent = str((rev_tpl or {}).get("parent_id", "1")).strip() or "1"
    rev_start = int((rev_tpl or {}).get("start_index", 2) or 2)
    rev_suffix = str((rev_tpl or {}).get("label_suffix", "收入")).strip() or "收入"

    gp_tpl = next((x for x in template_tree if str(x.get("template_name", "")).strip() == "gross_segment"), None)
    gp_prefix = str((gp_tpl or {}).get("node_id_prefix", "2.1.1.")).strip() or "2.1.1."
    gp_parent = str((gp_tpl or {}).get("parent_id", "2.1.1")).strip() or "2.1.1"
    gp_start = int((gp_tpl or {}).get("start_index", 1) or 1)
    gp_suffix = str((gp_tpl or {}).get("label_suffix", "毛利")).strip() or "毛利"

    # Build left tree strictly in configured order; template nodes are expanded in place.
    for rule in tree_rules:
        ntype = str(rule.get("node_type", "fixed")).strip().lower()
        if ntype == "fixed":
            nid = str(rule.get("node_id", "")).strip()
            label = str(rule.get("label", "")).strip()
            if nid and label:
                add_tree(nid, str(rule.get("parent_id", "")).strip(), label)
            continue
        if ntype != "template":
            continue
        tname = str(rule.get("template_name", "")).strip()
        if tname == "revenue_segment":
            for i, seg in enumerate(segments, start=0):
                nid = f"{rev_prefix}{rev_start + i}"
                label = f"{seg['label']}{rev_suffix}"
                add_tree(nid, rev_parent, label)
        elif tname == "gross_segment":
            for i, seg in enumerate(segments, start=0):
                nid = f"{gp_prefix}{gp_start + i}"
                label = f"{seg['label']}{gp_suffix}"
                add_tree(nid, gp_parent, label)

    # 1 营业收入分析
    main_rev = _pick_is_row(is_rows, ["IS001"], ["营业总收入", "营业收入", "主营业务收入"])
    main_rev_vals = main_rev["values"] if main_rev else _empty_year_values(years)
    if "1.1" in fixed_label_map:
        nodes.append(
            make_node(
                "1.1",
                fixed_parent_map.get("1.1", "1"),
                fixed_label_map.get("1.1", "营业收入总额"),
                main_rev_vals,
                main_rev["code"] if main_rev else "",
                main_rev["name"] if main_rev else "",
            )
        )

    for i, seg in enumerate(segments, start=0):
        nid = f"{rev_prefix}{rev_start + i}"
        label = f"{seg['label']}{rev_suffix}"
        nodes.append(make_node(nid, rev_parent, label, seg["revenue_values"], seg["segment_id"], seg["label"]))

    gross_vals = _sum_values([x["gross_values"] for x in segments], years)
    if "2.1.1" in fixed_label_map:
        nodes.append(
            make_node(
                "2.1.1",
                fixed_parent_map.get("2.1.1", "2.1"),
                fixed_label_map.get("2.1.1", "主营业务毛利（自动汇总）"),
                gross_vals,
            )
        )

    for i, seg in enumerate(segments, start=0):
        nid = f"{gp_prefix}{gp_start + i}"
        label = f"{seg['label']}{gp_suffix}"
        nodes.append(make_node(nid, gp_parent, label, seg["gross_values"], seg["segment_id"], seg["label"]))
    # For non-main revenue items, use name-based mapping first to avoid code-shift issues
    # across different templates/projects.
    other_cfg = income_rules.get("special_items", DEFAULT_INCOME_SPECIAL_ITEMS)
    for item in other_cfg:
        nid = str(item.get("node_id", "")).strip()
        label = str(item.get("label", "")).strip()
        codes = [str(x).strip() for x in item.get("code_candidates", []) if str(x).strip()]
        kws = [str(x).strip() for x in item.get("name_keywords", []) if str(x).strip()]
        if not nid or not label:
            continue
        parent_for_node = fixed_parent_map.get(nid, "2.1.2")
        if nid == "2.1.2.4":
            nodes.append(make_node(nid, parent_for_node, label, _empty_year_values(years)))
            continue
        p = _pick_is_row(is_rows, codes, kws)
        vals = p["values"] if p else _empty_year_values(years)
        nodes.append(make_node(nid, parent_for_node, label, vals, p["code"] if p else "", p["name"] if p else ""))

    p_asset = _pick_is_row(is_rows, [], ["资产处置收益"])
    v_asset = p_asset["values"] if p_asset else _empty_year_values(years)
    if "2.2.1" in fixed_label_map:
        nodes.append(
            make_node(
                "2.2.1",
                fixed_parent_map.get("2.2.1", "2.2"),
                fixed_label_map.get("2.2.1", "资产处置收益"),
                v_asset,
                p_asset["code"] if p_asset else "",
                p_asset["name"] if p_asset else "",
            )
        )

    p_noi = _pick_is_row(is_rows, [], ["营业外收入"])
    p_noe = _pick_is_row(is_rows, [], ["营业外支出"])
    v_noi = p_noi["values"] if p_noi else _empty_year_values(years)
    v_noe = p_noe["values"] if p_noe else _empty_year_values(years)
    v_net = {y: ((v_noi.get(y) or 0.0) - (v_noe.get(y) or 0.0)) if (v_noi.get(y) is not None or v_noe.get(y) is not None) else None for y in years}
    if "2.2.2" in fixed_label_map:
        nodes.append(
            make_node(
                "2.2.2",
                fixed_parent_map.get("2.2.2", "2.2"),
                fixed_label_map.get("2.2.2", "营业外收支净额（自动汇总）"),
                v_net,
            )
        )

    # 3 分析输出（公式驱动；缺失时回退默认分组逻辑）
    by_node = {n["node_id"]: n for n in nodes}
    formula_rules = income_rules.get("formula_rules", []) or []
    formula_map = {str(x.get("node_id", "")).strip(): str(x.get("formula", "")).strip() for x in formula_rules}
    if not formula_map:
        recurring_keys = set(income_rules.get("recurring_keys", DEFAULT_RECURRING_KEYS))
        nonrec_keys = set(income_rules.get("nonrec_keys", DEFAULT_NONREC_KEYS))
        formula_map = {
            "3.1": f"SUM({','.join(sorted(recurring_keys))})",
            "3.2": f"SUM({','.join(sorted(nonrec_keys))})",
            "3.3": "SUB(3.1,3.2)",
        }

    output_order = [nid for nid in [x.get("node_id", "") for x in fixed_tree] if str(nid).startswith("3.")]
    if not output_order:
        output_order = ["3.1", "3.2", "3.3"]
    for nid in output_order:
        nid = str(nid).strip()
        if not nid or nid not in fixed_label_map:
            continue
        vals = _eval_income_formula(formula_map.get(nid, ""), by_node, years)
        node = make_node(
            nid,
            fixed_parent_map.get(nid, "3"),
            fixed_label_map.get(nid, nid),
            vals,
        )
        nodes.append(node)
        by_node[nid] = node

    # Enrich segment-level auto text with structure ratio and trend:
    # - Revenue segment share in total revenue
    # - Segment gross contribution share in total gross
    by_node = {n["node_id"]: n for n in nodes}
    total_revenue_values = by_node.get("1.1", {}).get("values", _empty_year_values(years))
    total_gross_values = by_node.get("2.1.1", {}).get("values", _empty_year_values(years))
    for n in nodes:
        if n.get("parent_id") == "1" and n.get("node_id") != "1.1":
            ratio_txt = _fmt_ratio_auto_text(
                "占营业收入总额比例",
                n.get("values", {}),
                total_revenue_values,
                years,
                stable_pp=share_stable_pp,
                text_templates=income_text_templates,
            )
            n["auto_text"] = _append_text(n.get("auto_text", ""), ratio_txt)
        if n.get("parent_id") == "2.1.1":
            ratio_txt = _fmt_ratio_auto_text(
                "毛利贡献（占总毛利比例）",
                n.get("values", {}),
                total_gross_values,
                years,
                stable_pp=share_stable_pp,
                text_templates=income_text_templates,
            )
            n["auto_text"] = _append_text(n.get("auto_text", ""), ratio_txt)

    # Merge manual store
    store = load_store(project_id)
    entries = store.get("entries", {})
    for n in nodes:
        saved = entries.get(make_entry_key("income_analysis", n["node_id"]), {})
        manual = str(saved.get("manual_text", "") or "")
        confirmed = bool(saved.get("confirmed", False))
        n["manual_text"] = manual
        n["confirmed"] = confirmed
        n["final_text"] = manual if manual.strip() else n["auto_text"]

    return {
        "sheet_title": ws.title,
        "years": years,
        "tree": tree,
        "nodes": nodes,
    }


def read_generic_sheet(ws) -> Dict[str, Any]:
    headers = [str(ws.cell(1, c).value or "").strip() for c in range(1, ws.max_column + 1)]
    rows: List[Dict[str, Any]] = []
    for r in range(2, ws.max_row + 1):
        values = [ws.cell(r, c).value for c in range(1, ws.max_column + 1)]
        if all(v in (None, "") for v in values):
            continue
        obj: Dict[str, Any] = {}
        for i, h in enumerate(headers):
            key = h or f"col_{i+1}"
            obj[key] = values[i]
        rows.append(obj)
    return {"sheet_title": ws.title, "headers": headers, "rows": rows}


def read_analysis_data(wb, sheet_map: Dict[str, List[str]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, candidates in sheet_map.items():
        ws = get_sheet_by_loose_name(wb, candidates)
        if ws is None:
            out[k] = {"sheet_title": None, "headers": [], "rows": []}
            continue
        out[k] = read_generic_sheet(ws)
    return out


def build_analysis_map(wb, project_id: str, sheet_map: Dict[str, List[str]], group_id: str) -> Dict[str, Dict[str, Any]]:
    data = read_analysis_data(wb, sheet_map)
    data = apply_analysis_code_redirects(group_id, data)
    scale_rows = data.get("scale", {}).get("rows", []) or []
    struct_rows = data.get("structure", {}).get("rows", []) or []

    scale_by_code = {str(r.get("科目编码", "")).strip(): r for r in scale_rows}
    struct_by_code = {str(r.get("科目编码", "")).strip(): r for r in struct_rows}

    store = load_store(project_id)
    entries = store.get("entries", {})
    out: Dict[str, Dict[str, Any]] = {}

    all_codes = set(scale_by_code.keys()) | set(struct_by_code.keys())
    for code in all_codes:
        if not code:
            continue
        scale_row = scale_by_code.get(code, {})
        struct_row = struct_by_code.get(code, {})
        auto_abs = str(scale_row.get("定量描述_绝对量", "") or "")
        auto_rel = str(struct_row.get("定量描述_相对量", "") or "")
        auto_combined = auto_abs if not auto_rel else f"{auto_abs}\n{auto_rel}"

        saved = entries.get(make_entry_key(group_id, code), {})
        manual_text = str(saved.get("manual_text", "") or "")
        confirmed = bool(saved.get("confirmed", False))
        # BS主页面只展示“分析页可编辑框”的最终文本：
        # 有手工保存内容时直接展示该内容；否则回退到自动文本。
        final_text = manual_text if manual_text.strip() else auto_combined

        out[code] = {
            "code": code,
            "name": str(scale_row.get("科目名称", "") or struct_row.get("科目名称", "")),
            "auto_abs": auto_abs,
            "auto_rel": auto_rel,
            "auto_combined": auto_combined,
            "manual_text": manual_text,
            "confirmed": confirmed,
            "final_text": final_text,
        }
    return out


def apply_analysis_code_redirects(group_id: str, analysis_data: Dict[str, Any]) -> Dict[str, Any]:
    redirect_map = ANALYSIS_CODE_REDIRECTS.get(group_id, {})
    if not redirect_map:
        return analysis_data

    for section in ("scale", "structure"):
        part = analysis_data.get(section, {}) if isinstance(analysis_data, dict) else {}
        rows = part.get("rows", []) if isinstance(part, dict) else []
        if not isinstance(rows, list) or not rows:
            continue

        idx_by_code: Dict[str, int] = {}
        for i, row in enumerate(rows):
            if not isinstance(row, dict):
                continue
            code = str(row.get("科目编码", "")).strip()
            if code:
                idx_by_code[code] = i

        for to_code, from_code in redirect_map.items():
            i_from = idx_by_code.get(str(from_code).strip())
            if i_from is None:
                continue
            from_row = rows[i_from]
            to_code = str(to_code).strip()
            i_to = idx_by_code.get(to_code)
            to_row = rows[i_to] if i_to is not None else {}

            merged = dict(from_row)
            merged["科目编码"] = to_code
            # Keep destination subject name if present; fallback to source name.
            dst_name = str((to_row or {}).get("科目名称", "")).strip()
            if dst_name:
                merged["科目名称"] = dst_name

            if i_to is not None:
                rows[i_to] = merged
            else:
                rows.append(merged)
    return analysis_data


def build_asset_analysis_map(wb, project_id: str) -> Dict[str, Dict[str, Any]]:
    return build_analysis_map(wb, project_id, ASSET_ANALYSIS_SHEETS, "asset_analysis")


def build_liability_analysis_map(wb, project_id: str) -> Dict[str, Dict[str, Any]]:
    return build_analysis_map(wb, project_id, LIABILITY_ANALYSIS_SHEETS, "liability_analysis")


def load_store(project_id: str) -> Dict[str, Any]:
    p = store_path(project_id)
    if not p.exists():
        return {"project_id": normalize_project_id(project_id), "entries": {}}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"project_id": normalize_project_id(project_id), "entries": {}}


def save_store(project_id: str, payload: Dict[str, Any]) -> None:
    p = store_path(project_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def make_entry_key(group_id: str, code: str) -> str:
    return f"{group_id}:{code}"


def generate_auto_text(name: str, values: Dict[str, Optional[float]], years: List[str]) -> str:
    tpl = load_key_ratio_text_rules().get("templates", {})
    if not years:
        return ""
    latest = years[-1]
    latest_val = values.get(latest)
    if latest_val is None:
        return _render_template(
            tpl.get("sheet_auto_no_latest", DEFAULT_TEXT_TEMPLATES["sheet_auto_no_latest"]),
            {"name": name},
        )
    if len(years) < 2:
        return _render_template(
            tpl.get("sheet_auto_one_year", DEFAULT_TEXT_TEMPLATES["sheet_auto_one_year"]),
            {"name": name, "latest": latest, "latest_val": f"{latest_val:,.2f}"},
        )

    prev = years[-2]
    prev_val = values.get(prev)
    if prev_val in (None, 0):
        return _render_template(
            tpl.get("sheet_auto_prev_missing", DEFAULT_TEXT_TEMPLATES["sheet_auto_prev_missing"]),
            {"name": name, "latest": latest, "latest_val": f"{latest_val:,.2f}"},
        )

    diff = latest_val - prev_val
    pct = diff / abs(prev_val) * 100
    direction = "上升" if diff >= 0 else "下降"
    return _render_template(
        tpl.get("sheet_auto_two_year", DEFAULT_TEXT_TEMPLATES["sheet_auto_two_year"]),
        {
            "name": name,
            "latest": latest,
            "prev": prev,
            "latest_val": f"{latest_val:,.2f}",
            "direction": direction,
            "abs_diff": f"{abs(diff):,.2f}",
            "abs_pct": f"{abs(pct):.2f}",
        },
    )


def list_projects() -> List[str]:
    out_root = PROJECT_ROOT / "outputs"
    if not out_root.exists():
        return []
    return sorted([p.name for p in out_root.iterdir() if p.is_dir()])


def render_index() -> str:
    cards = "".join(
        f'<a class="card" href="/sheet/{g["id"]}"><h3>{g["label"]}</h3><p>进入查看与编辑</p></a>' for g in SHEET_GROUPS
    )
    analysis_cards = (
        '<a class="card" href="/analysis/assets"><h3>资产分析</h3><p>查看规模变化与结构占比</p></a>'
        '<a class="card" href="/analysis/liabilities"><h3>负债分析</h3><p>查看规模变化与结构占比</p></a>'
        '<a class="card" href="/analysis/income"><h3>收入分析</h3><p>经常性/非经常性收益拆分</p></a>'
        '<a class="card" href="/analysis/ratios"><h3>财务指标分析</h3><p>指标趋势、判断与确认</p></a>'
        '<a class="card" href="/analysis/key-ratios"><h3>重要指标分析</h3><p>ROE与毛利率深度分析</p></a>'
    )
    project_options = "".join(f'<option value="{p}">{p}</option>' for p in list_projects())
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>主文件前端</title>
  <style>
    :root {{ --bg:#f3f5f7; --card:#ffffff; --ink:#111827; --muted:#6b7280; --line:#d1d5db; --acc:#0f766e; }}
    body {{ margin:0; font-family: "Microsoft YaHei", "PingFang SC", sans-serif; background:var(--bg); color:var(--ink); }}
    .wrap {{ max-width:1000px; margin:28px auto; padding:0 16px; }}
    h1 {{ margin:0 0 8px; }}
    .meta {{ color:var(--muted); margin-bottom:14px; }}
    .toolbar {{ background:#fff; border:1px solid var(--line); border-radius:10px; padding:10px; margin-bottom:14px; }}
    .grid {{ display:grid; grid-template-columns: repeat(auto-fit,minmax(220px,1fr)); gap:14px; }}
    .card {{ display:block; text-decoration:none; background:var(--card); border:1px solid var(--line); border-radius:10px; padding:18px; color:inherit; }}
    .card:hover {{ border-color:var(--acc); box-shadow:0 3px 10px rgba(0,0,0,.05); }}
    input,select,button {{ font:inherit; }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>主文件展示与确认</h1>
    <div class="meta">一级页面：基础报表 + 分析视图</div>
    <div class="toolbar">
      <label>项目ID：</label>
      <input id="pid" value="{DEFAULT_PROJECT_ID}" style="min-width:260px;padding:6px;"/>
      <select id="plist"><option value="">从 outputs 选择</option>{project_options}</select>
      <button onclick="applyProject()">应用</button>
      <span id="tip" style="margin-left:8px;color:#6b7280;"></span>
    </div>
    <div class="grid" id="cards">{cards}{analysis_cards}</div>
  </div>
<script>
function getPid() {{
  return (document.getElementById('pid').value || '').trim();
}}
function rewriteLinks() {{
  const pid = getPid();
  document.querySelectorAll('#cards a.card').forEach(a => {{
    const u = new URL(a.getAttribute('href'), location.origin);
    if (pid) u.searchParams.set('project_id', pid);
    a.setAttribute('href', u.pathname + u.search);
  }});
  document.getElementById('tip').textContent = pid ? ('当前项目: ' + pid) : '未指定项目ID';
}}
function applyProject() {{
  const plist = document.getElementById('plist').value;
  if (plist) document.getElementById('pid').value = plist;
  const pid = getPid();
  const u = new URL(location.href);
  if (pid) {{
    u.searchParams.set('project_id', pid);
  }} else {{
    u.searchParams.delete('project_id');
  }}
  location.href = u.pathname + u.search;
}}
const qpid = new URLSearchParams(location.search).get('project_id');
if (qpid) document.getElementById('pid').value = qpid;
document.getElementById('plist').addEventListener('change', () => {{
  const v = document.getElementById('plist').value;
  if (v) {{
    document.getElementById('pid').value = v;
    rewriteLinks();
  }}
}});
document.getElementById('pid').addEventListener('input', rewriteLinks);
rewriteLinks();
</script>
</body>
</html>"""


def render_analysis_page(project_id: str, title: str, api_path: str, save_group_id: str, subject_label: str) -> str:
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{title}</title>
  <style>
    :root {{ --bg:#f5f7fa; --card:#fff; --ink:#111827; --line:#d1d5db; --acc:#0f766e; --muted:#6b7280; }}
    body {{ margin:0; font-family: "Microsoft YaHei", "PingFang SC", sans-serif; background:var(--bg); color:var(--ink); }}
    .top {{ position:sticky; top:0; background:#fff; border-bottom:1px solid var(--line); padding:10px 16px; display:flex; gap:8px; align-items:center; flex-wrap:wrap; }}
    .btn {{ border:1px solid var(--line); background:#fff; border-radius:8px; padding:6px 10px; text-decoration:none; color:inherit; cursor:pointer; }}
    .btn.primary {{ background:var(--acc); color:#fff; border-color:var(--acc); }}
    .muted {{ color:var(--muted); font-size:12px; }}
    .wrap {{ display:grid; grid-template-columns:280px 1fr; gap:12px; padding:12px 16px 20px; }}
    .panel {{ background:var(--card); border:1px solid var(--line); border-radius:10px; overflow:hidden; }}
    .panel h3 {{ margin:0; font-size:14px; padding:10px 12px; border-bottom:1px solid var(--line); background:#fafafa; }}
    .list {{ max-height:calc(100vh - 130px); overflow:auto; }}
    .item {{ padding:8px 10px; border-bottom:1px solid #eef2f7; cursor:pointer; }}
    .item:hover {{ background:#f8fafc; }}
    .item.active {{ background:#e6fffa; border-left:3px solid var(--acc); }}
    .cards {{ display:grid; grid-template-columns:1fr; gap:12px; }}
    .table-wrap {{ overflow:auto; }}
    table {{ width:100%; border-collapse:collapse; }}
    th,td {{ font-size:12px; border-bottom:1px solid #eef2f7; padding:6px 8px; text-align:left; }}
    td.num {{ text-align:right; white-space:nowrap; }}
    .desc {{ white-space:pre-wrap; font-size:13px; line-height:1.5; padding:10px 12px; }}
    textarea {{ width:100%; min-height:170px; box-sizing:border-box; border:1px solid #d1d5db; border-radius:8px; padding:10px; font:inherit; }}
    .toolbar {{ padding:10px 12px; display:flex; gap:10px; align-items:center; flex-wrap:wrap; border-top:1px solid #eef2f7; }}
    .ok {{ color:#065f46; font-weight:600; }}
  </style>
</head>
<body>
  <div class="top">
    <a class="btn" href="/?project_id={project_id}">返回主页</a>
    <strong>{title}</strong>
    <span class="muted">项目ID: {project_id}</span>
    <span class="muted" id="status"></span>
    <button class="btn primary" onclick="reloadAll()">刷新数据</button>
  </div>
  <div class="wrap">
    <div class="panel">
      <h3>{subject_label}列表</h3>
      <div class="list" id="subjectList"></div>
    </div>
    <div class="cards">
      <div class="panel">
        <h3>自动分析（只读）</h3>
        <div class="table-wrap"><table id="scaleTbl"></table></div>
        <div class="desc" id="scaleDesc"></div>
        <div class="desc" id="structDesc"></div>
      </div>
      <div class="panel">
        <h3>分析判断内容（可编辑）</h3>
        <div class="desc"><textarea id="manualText"></textarea></div>
        <div class="toolbar">
          <label><input type="checkbox" id="manualConfirmed"> 确认</label>
          <button class="btn primary" onclick="saveActive()">保存并确认</button>
          <span id="saveHint" class="muted"></span>
        </div>
      </div>
    </div>
  </div>
<script>
const PROJECT_ID = {json.dumps(project_id)};
const API_PATH = {json.dumps(api_path)};
const SAVE_GROUP_ID = {json.dumps(save_group_id)};
let cache = null;
let activeCode = '';

function esc(s) {{
  return (s ?? '').toString().replace(/[&<>"]/g, c => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}}[c]));
}}

function rowByCode(rows, code) {{
  return (rows || []).find(r => String((r['科目编码'] ?? r['code'] ?? '')) === code) || null;
}}

function numericCell(v) {{
  if (typeof v === 'number') return `<td class="num">${{v.toLocaleString(undefined, {{maximumFractionDigits: 4}})}}</td>`;
  return `<td>${{esc(v)}}</td>`;
}}

function renderRowTable(elId, rowObj) {{
  const el = document.getElementById(elId);
  if (!rowObj) {{
    el.innerHTML = '<tr><td>未找到该科目数据</td></tr>';
    return;
  }}
  const keys = Object.keys(rowObj);
  const head = `<tr>${{keys.map(k => `<th>${{esc(k)}}</th>`).join('')}}</tr>`;
  const body = `<tr>${{keys.map(k => numericCell(rowObj[k])).join('')}}</tr>`;
  el.innerHTML = head + body;
}}

function renderSubjectList() {{
  const rows = cache.scale.rows || [];
  const html = rows.map(r => {{
    const c = String(r['科目编码'] || '');
    const n = String(r['科目名称'] || '');
    const active = c === activeCode ? 'active' : '';
    return `<div class="item ${{active}}" data-code="${{esc(c)}}"><strong>${{esc(c)}}</strong> ${{esc(n)}}</div>`;
  }}).join('');
  const box = document.getElementById('subjectList');
  box.innerHTML = html || '<div class="item">暂无数据</div>';
  box.querySelectorAll('.item[data-code]').forEach(node => {{
    node.addEventListener('click', () => {{
      activeCode = node.dataset.code;
      renderAll();
    }});
  }});
}}

function renderAll() {{
  renderSubjectList();
  const scaleRow = rowByCode(cache.scale.rows, activeCode);
  const structRow = rowByCode(cache.structure.rows, activeCode);
  const analysisRow = rowByCode(cache.analysis.rows, activeCode) || {{}};
  renderRowTable('scaleTbl', scaleRow);
  document.getElementById('scaleDesc').textContent = scaleRow?.['定量描述_绝对量'] || '';
  document.getElementById('structDesc').textContent = structRow?.['定量描述_相对量'] || '';
  document.getElementById('manualText').value = analysisRow['manual_text'] || analysisRow['auto_combined'] || '';
  document.getElementById('manualConfirmed').checked = !!analysisRow['confirmed'];
  document.getElementById('saveHint').textContent = analysisRow['confirmed'] ? '当前已确认' : '';
  document.getElementById('status').textContent = `规模表行数:${{cache.scale.rows.length}} | 结构表行数:${{cache.structure.rows.length}}`;
}}

async function saveActive() {{
  if (!activeCode) return;
  const scaleRow = rowByCode(cache.scale.rows, activeCode) || {{}};
  const body = {{
    project_id: PROJECT_ID,
    group_id: SAVE_GROUP_ID,
    rows: [{{
      code: activeCode,
      name: String(scaleRow['科目名称'] || ''),
      manual_text: document.getElementById('manualText').value,
      confirmed: document.getElementById('manualConfirmed').checked
    }}]
  }};
  const res = await fetch('/api/save', {{
    method: 'POST',
    headers: {{'Content-Type':'application/json'}},
    body: JSON.stringify(body)
  }});
  const data = await res.json();
  if (data.ok) {{
    document.getElementById('saveHint').textContent = '已保存';
    await reloadAll();
  }} else {{
    document.getElementById('saveHint').textContent = '保存失败';
  }}
}}

async function reloadAll() {{
  const res = await fetch(`${{API_PATH}}?project_id=${{encodeURIComponent(PROJECT_ID)}}`);
  const data = await res.json();
  if (!res.ok) {{
    document.getElementById('status').textContent = data.error || '读取失败';
    return;
  }}
  cache = data;
  if (!activeCode && cache.scale.rows.length) {{
    activeCode = String(cache.scale.rows[0]['科目编码'] || '');
  }}
  renderAll();
}}

reloadAll();
</script>
</body>
</html>"""


def render_asset_analysis_page(project_id: str) -> str:
    return render_analysis_page(
        project_id=project_id,
        title="资产分析",
        api_path="/api/analysis/assets",
        save_group_id="asset_analysis",
        subject_label="资产科目",
    )


def render_liability_analysis_page(project_id: str) -> str:
    return render_analysis_page(
        project_id=project_id,
        title="负债分析",
        api_path="/api/analysis/liabilities",
        save_group_id="liability_analysis",
        subject_label="负债科目",
    )


def render_income_analysis_page(project_id: str) -> str:
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>收入分析</title>
  <style>
    :root {{ --bg:#f6f8fb; --card:#fff; --ink:#111827; --line:#d1d5db; --muted:#6b7280; --acc:#0f766e; }}
    body {{ margin:0; font-family: "Microsoft YaHei", "PingFang SC", sans-serif; background:var(--bg); color:var(--ink); }}
    .top {{ position:sticky; top:0; background:#fff; border-bottom:1px solid var(--line); padding:10px 16px; display:flex; gap:10px; align-items:center; flex-wrap:wrap; }}
    .btn {{ border:1px solid var(--line); background:#fff; border-radius:8px; padding:6px 10px; text-decoration:none; color:inherit; cursor:pointer; }}
    .btn.primary {{ background:var(--acc); color:#fff; border-color:var(--acc); }}
    .muted {{ color:var(--muted); font-size:12px; }}
    .wrap {{ display:grid; grid-template-columns:320px 1fr; gap:12px; margin:12px auto; padding:0 16px 20px; }}
    .panel {{ background:var(--card); border:1px solid var(--line); border-radius:10px; overflow:hidden; }}
    .panel h3 {{ margin:0; padding:10px 12px; border-bottom:1px solid var(--line); background:#fafafa; font-size:14px; }}
    .tree {{ max-height:calc(100vh - 130px); overflow:auto; }}
    .node {{ padding:7px 10px; border-bottom:1px solid #eef2f7; cursor:pointer; font-size:13px; }}
    .node:hover {{ background:#f8fafc; }}
    .node.active {{ background:#e6fffa; border-left:3px solid var(--acc); }}
    .cards {{ display:grid; grid-template-columns:1fr; gap:12px; }}
    .desc {{ white-space:pre-wrap; font-size:13px; line-height:1.5; padding:10px 12px; }}
    textarea {{ width:100%; min-height:180px; box-sizing:border-box; border:1px solid #d1d5db; border-radius:8px; padding:10px; font:inherit; }}
    .toolbar {{ padding:10px 12px; display:flex; gap:10px; align-items:center; flex-wrap:wrap; border-top:1px solid #eef2f7; }}
  </style>
</head>
<body>
  <div class="top">
    <a class="btn" href="/?project_id={project_id}">返回主页</a>
    <strong>收入分析</strong>
    <span class="muted">项目ID: {project_id}</span>
    <span class="muted" id="status"></span>
    <button class="btn" onclick="exportExcel()">导出Excel计算表</button>
    <button class="btn primary" onclick="reloadData()">刷新</button>
  </div>
  <div class="wrap">
    <div class="panel">
      <h3>收入分析树</h3>
      <div class="tree" id="treeBox"></div>
    </div>
    <div class="cards">
      <div class="panel">
        <h3 id="nodeTitle">自动分析（只读）</h3>
        <div class="desc" id="autoText"></div>
      </div>
      <div class="panel">
        <h3>分析判断内容（可编辑）</h3>
        <div class="desc"><textarea id="manualText"></textarea></div>
        <div class="toolbar">
          <label><input type="checkbox" id="manualConfirmed"> 确认</label>
          <button class="btn primary" onclick="saveActive()">保存并确认</button>
          <span id="saveHint" class="muted"></span>
        </div>
      </div>
    </div>
  </div>
<script>
const PROJECT_ID = {json.dumps(project_id)};
let cache = null;
let activeNodeId = '';

function esc(s) {{
  return (s ?? '').toString().replace(/[&<>"]/g, c => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}}[c]));
}}
function depthOf(id) {{
  return (id || '').split('.').length - 1;
}}
function nodeById(id) {{
  return (cache.nodes || []).find(x => String(x.node_id) === String(id)) || null;
}}
function renderTree() {{
  const tree = cache.tree || [];
  const box = document.getElementById('treeBox');
  box.innerHTML = tree.map(n => {{
    const d = depthOf(n.node_id);
    const active = n.node_id === activeNodeId ? 'active' : '';
    return `<div class="node ${{active}}" data-id="${{esc(n.node_id)}}" style="padding-left:${{10 + d * 16}}px;">${{esc(n.node_id)}} ${{esc(n.label)}}</div>`;
  }}).join('');
  box.querySelectorAll('.node[data-id]').forEach(el => {{
    el.addEventListener('click', () => {{
      activeNodeId = el.dataset.id;
      renderNode();
    }});
  }});
}}
function renderNode() {{
  renderTree();
  const n = nodeById(activeNodeId);
  if (!n) return;
  document.getElementById('nodeTitle').textContent = `${{n.node_id}} ${{n.label}}（自动分析）`;
  document.getElementById('autoText').textContent = n.auto_text || '';
  document.getElementById('manualText').value = n.manual_text || n.auto_text || '';
  document.getElementById('manualConfirmed').checked = !!n.confirmed;
  document.getElementById('saveHint').textContent = n.confirmed ? '当前已确认' : '';
}}
async function saveActive() {{
  const n = nodeById(activeNodeId);
  if (!n) return;
  const payload = {{
    project_id: PROJECT_ID,
    group_id: 'income_analysis',
    rows: [{{
      code: n.node_id,
      name: n.label,
      manual_text: document.getElementById('manualText').value,
      confirmed: document.getElementById('manualConfirmed').checked
    }}]
  }};
  const res = await fetch('/api/save', {{
    method: 'POST',
    headers: {{'Content-Type':'application/json'}},
    body: JSON.stringify(payload)
  }});
  const data = await res.json();
  document.getElementById('saveHint').textContent = data.ok ? '已保存' : '保存失败';
  if (data.ok) await reloadData();
}}
async function reloadData() {{
  const res = await fetch(`/api/analysis/income?project_id=${{encodeURIComponent(PROJECT_ID)}}`);
  cache = await res.json();
  if (!res.ok) {{
    document.getElementById('status').textContent = cache.error || '读取失败';
    return;
  }}
  if (!activeNodeId && (cache.tree || []).length) {{
    activeNodeId = cache.tree[0].node_id;
  }}
  renderNode();
  document.getElementById('status').textContent = `来源Sheet: ${{cache.sheet_title || '-'}} | 年份: ${{(cache.years||[]).join(',')}}`;
}}
function exportExcel() {{
  const url = `/api/analysis/income/export?project_id=${{encodeURIComponent(PROJECT_ID)}}`;
  window.location.href = url;
}}
reloadData();
</script>
</body>
</html>"""


def render_ratio_analysis_page(project_id: str) -> str:
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>财务指标分析</title>
  <style>
    :root {{ --bg:#f6f8fb; --card:#fff; --ink:#111827; --line:#d1d5db; --muted:#6b7280; --acc:#0f766e; }}
    body {{ margin:0; font-family: "Microsoft YaHei", "PingFang SC", sans-serif; background:var(--bg); color:var(--ink); }}
    .top {{ position:sticky; top:0; background:#fff; border-bottom:1px solid var(--line); padding:10px 16px; display:flex; gap:10px; align-items:center; flex-wrap:wrap; }}
    .btn {{ border:1px solid var(--line); background:#fff; border-radius:8px; padding:6px 10px; text-decoration:none; color:inherit; cursor:pointer; }}
    .btn.primary {{ background:var(--acc); color:#fff; border-color:var(--acc); }}
    .muted {{ color:var(--muted); font-size:12px; }}
    .wrap {{ display:grid; grid-template-columns:320px 1fr; gap:12px; margin:12px auto; padding:0 16px 20px; }}
    .panel {{ background:var(--card); border:1px solid var(--line); border-radius:10px; overflow:hidden; }}
    .panel h3 {{ margin:0; padding:10px 12px; border-bottom:1px solid var(--line); background:#fafafa; font-size:14px; }}
    .tree {{ max-height:calc(100vh - 130px); overflow:auto; }}
    .node {{ padding:7px 10px; border-bottom:1px solid #eef2f7; cursor:pointer; font-size:13px; }}
    .node:hover {{ background:#f8fafc; }}
    .node.active {{ background:#e6fffa; border-left:3px solid var(--acc); }}
    .cards {{ display:grid; grid-template-columns:1fr; gap:12px; }}
    .desc {{ white-space:pre-wrap; font-size:13px; line-height:1.5; padding:10px 12px; }}
    textarea {{ width:100%; min-height:180px; box-sizing:border-box; border:1px solid #d1d5db; border-radius:8px; padding:10px; font:inherit; }}
    .toolbar {{ padding:10px 12px; display:flex; gap:10px; align-items:center; flex-wrap:wrap; border-top:1px solid #eef2f7; }}
  </style>
</head>
<body>
  <div class="top">
    <a class="btn" href="/?project_id={project_id}">返回主页</a>
    <strong>财务指标分析</strong>
    <span class="muted">项目ID: {project_id}</span>
    <span class="muted" id="status"></span>
    <button class="btn primary" onclick="reloadData()">刷新</button>
  </div>
  <div class="wrap">
    <div class="panel">
      <h3>指标分析树</h3>
      <div class="tree" id="treeBox"></div>
    </div>
    <div class="cards">
      <div class="panel">
        <h3 id="nodeTitle">自动分析（只读）</h3>
        <div class="desc" id="autoText"></div>
      </div>
      <div class="panel">
        <h3>分析判断内容（可编辑）</h3>
        <div class="desc"><textarea id="manualText"></textarea></div>
        <div class="toolbar">
          <label><input type="checkbox" id="manualConfirmed"> 确认</label>
          <button class="btn primary" onclick="saveActive()">保存并确认</button>
          <span id="saveHint" class="muted"></span>
        </div>
      </div>
    </div>
  </div>
<script>
const PROJECT_ID = {json.dumps(project_id)};
let cache = null;
let activeNodeId = '';

function esc(s) {{
  return (s ?? '').toString().replace(/[&<>"]/g, c => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}}[c]));
}}
function depthOf(id) {{
  return (id || '').split('.').length - 1;
}}
function nodeById(id) {{
  return (cache.nodes || []).find(x => String(x.node_id) === String(id)) || null;
}}
function renderTree() {{
  const tree = cache.tree || [];
  const box = document.getElementById('treeBox');
  box.innerHTML = tree.map(n => {{
    const d = depthOf(n.node_id);
    const active = n.node_id === activeNodeId ? 'active' : '';
    return `<div class="node ${{active}}" data-id="${{esc(n.node_id)}}" style="padding-left:${{10 + d * 16}}px;">${{esc(n.node_id)}} ${{esc(n.label)}}</div>`;
  }}).join('');
  box.querySelectorAll('.node[data-id]').forEach(el => {{
    el.addEventListener('click', () => {{
      activeNodeId = el.dataset.id;
      renderNode();
    }});
  }});
}}
function renderNode() {{
  renderTree();
  const n = nodeById(activeNodeId);
  if (!n) return;
  document.getElementById('nodeTitle').textContent = `${{n.node_id}} ${{n.label}}（自动分析）`;
  document.getElementById('autoText').textContent = n.auto_text || '';
  document.getElementById('manualText').value = n.manual_text || n.auto_text || '';
  document.getElementById('manualConfirmed').checked = !!n.confirmed;
  document.getElementById('saveHint').textContent = n.confirmed ? '当前已确认' : '';
}}
async function saveActive() {{
  const n = nodeById(activeNodeId);
  if (!n) return;
  const payload = {{
    project_id: PROJECT_ID,
    group_id: 'ratio_analysis',
    rows: [{{
      code: n.node_id,
      name: n.label,
      manual_text: document.getElementById('manualText').value,
      confirmed: document.getElementById('manualConfirmed').checked
    }}]
  }};
  const res = await fetch('/api/save', {{
    method: 'POST',
    headers: {{'Content-Type':'application/json'}},
    body: JSON.stringify(payload)
  }});
  const data = await res.json();
  document.getElementById('saveHint').textContent = data.ok ? '已保存' : '保存失败';
  if (data.ok) await reloadData();
}}
async function reloadData() {{
  const res = await fetch(`/api/analysis/ratios?project_id=${{encodeURIComponent(PROJECT_ID)}}`);
  cache = await res.json();
  if (!res.ok) {{
    document.getElementById('status').textContent = cache.error || '读取失败';
    return;
  }}
  if (!activeNodeId && (cache.tree || []).length) {{
    activeNodeId = cache.tree[0].node_id;
  }}
  renderNode();
  document.getElementById('status').textContent = `来源Sheet: ${{cache.sheet_title || '-'}} | 年份: ${{(cache.years||[]).join(',')}}`;
}}
reloadData();
</script>
</body>
</html>"""


def render_key_ratio_analysis_page(project_id: str) -> str:
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>重要指标分析</title>
  <style>
    :root {{ --bg:#f6f8fb; --card:#fff; --ink:#111827; --line:#d1d5db; --muted:#6b7280; --acc:#0f766e; }}
    body {{ margin:0; font-family: "Microsoft YaHei", "PingFang SC", sans-serif; background:var(--bg); color:var(--ink); }}
    .top {{ position:sticky; top:0; background:#fff; border-bottom:1px solid var(--line); padding:10px 16px; display:flex; gap:10px; align-items:center; flex-wrap:wrap; }}
    .btn {{ border:1px solid var(--line); background:#fff; border-radius:8px; padding:6px 10px; text-decoration:none; color:inherit; cursor:pointer; }}
    .btn.primary {{ background:var(--acc); color:#fff; border-color:var(--acc); }}
    .muted {{ color:var(--muted); font-size:12px; }}
    .wrap {{ display:grid; grid-template-columns:320px 1fr; gap:12px; margin:12px auto; padding:0 16px 20px; }}
    .panel {{ background:var(--card); border:1px solid var(--line); border-radius:10px; overflow:hidden; }}
    .panel h3 {{ margin:0; padding:10px 12px; border-bottom:1px solid var(--line); background:#fafafa; font-size:14px; }}
    .tree {{ max-height:calc(100vh - 130px); overflow:auto; }}
    .node {{ padding:7px 10px; border-bottom:1px solid #eef2f7; cursor:pointer; font-size:13px; }}
    .node:hover {{ background:#f8fafc; }}
    .node.active {{ background:#e6fffa; border-left:3px solid var(--acc); }}
    .cards {{ display:grid; grid-template-columns:1fr; gap:12px; }}
    .desc {{ white-space:pre-wrap; font-size:13px; line-height:1.6; padding:10px 12px; }}
    textarea {{ width:100%; min-height:200px; box-sizing:border-box; border:1px solid #d1d5db; border-radius:8px; padding:10px; font:inherit; }}
    .toolbar {{ padding:10px 12px; display:flex; gap:10px; align-items:center; flex-wrap:wrap; border-top:1px solid #eef2f7; }}
  </style>
</head>
<body>
  <div class="top">
    <a class="btn" href="/?project_id={project_id}">返回主页</a>
    <strong>重要指标分析</strong>
    <span class="muted">项目ID: {project_id}</span>
    <span class="muted" id="status"></span>
    <button class="btn primary" onclick="reloadData()">刷新</button>
  </div>
  <div class="wrap">
    <div class="panel">
      <h3>重点指标列表</h3>
      <div class="tree" id="treeBox"></div>
    </div>
    <div class="cards">
      <div class="panel">
        <h3 id="nodeTitle">自动分析（只读）</h3>
        <div class="desc" id="autoText"></div>
      </div>
      <div class="panel">
        <h3>分析判断内容（可编辑）</h3>
        <div class="desc"><textarea id="manualText"></textarea></div>
        <div class="toolbar">
          <label><input type="checkbox" id="manualConfirmed"> 确认</label>
          <button class="btn primary" onclick="saveActive()">保存并确认</button>
          <span id="saveHint" class="muted"></span>
        </div>
      </div>
    </div>
  </div>
<script>
const PROJECT_ID = {json.dumps(project_id)};
let cache = null;
let activeNodeId = '';

function esc(s) {{
  return (s ?? '').toString().replace(/[&<>"]/g, c => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}}[c]));
}}
function nodeById(id) {{
  return (cache.nodes || []).find(x => String(x.node_id) === String(id)) || null;
}}
function renderTree() {{
  const tree = cache.tree || [];
  const box = document.getElementById('treeBox');
  box.innerHTML = tree.map(n => {{
    const active = n.node_id === activeNodeId ? 'active' : '';
    return `<div class="node ${{active}}" data-id="${{esc(n.node_id)}}">${{esc(n.node_id)}} ${{esc(n.label)}}</div>`;
  }}).join('');
  box.querySelectorAll('.node[data-id]').forEach(el => {{
    el.addEventListener('click', () => {{
      activeNodeId = el.dataset.id;
      renderNode();
    }});
  }});
}}
function renderNode() {{
  renderTree();
  const n = nodeById(activeNodeId);
  if (!n) return;
  document.getElementById('nodeTitle').textContent = `${{n.node_id}} ${{n.label}}（自动分析）`;
  document.getElementById('autoText').textContent = n.auto_text || '';
  document.getElementById('manualText').value = n.manual_text || n.auto_text || '';
  document.getElementById('manualConfirmed').checked = !!n.confirmed;
  document.getElementById('saveHint').textContent = n.confirmed ? '当前已确认' : '';
}}
async function saveActive() {{
  const n = nodeById(activeNodeId);
  if (!n) return;
  const payload = {{
    project_id: PROJECT_ID,
    group_id: 'key_ratio_analysis',
    rows: [{{
      code: n.node_id,
      name: n.label,
      manual_text: document.getElementById('manualText').value,
      confirmed: document.getElementById('manualConfirmed').checked
    }}]
  }};
  const res = await fetch('/api/save', {{
    method: 'POST',
    headers: {{'Content-Type':'application/json'}},
    body: JSON.stringify(payload)
  }});
  const data = await res.json();
  document.getElementById('saveHint').textContent = data.ok ? '已保存' : '保存失败';
  if (data.ok) await reloadData();
}}
async function reloadData() {{
  const res = await fetch(`/api/analysis/key-ratios?project_id=${{encodeURIComponent(PROJECT_ID)}}`);
  cache = await res.json();
  if (!res.ok) {{
    document.getElementById('status').textContent = cache.error || '读取失败';
    return;
  }}
  if (!activeNodeId && (cache.tree || []).length) {{
    activeNodeId = cache.tree[0].node_id;
  }}
  renderNode();
  document.getElementById('status').textContent = `来源Sheet: ${{cache.sheet_title || '-'}} | 年份: ${{(cache.years||[]).join(',')}}`;
}}
reloadData();
</script>
</body>
</html>"""


def build_income_analysis_export_xlsx(data: Dict[str, Any], project_id: str, source_workbook: str) -> bytes:
    wb = Workbook()
    ws_nodes = wb.active
    ws_nodes.title = "收入分析_节点明细"
    years = [str(y) for y in (data.get("years", []) or [])]

    ws_nodes.append(["项目ID", project_id])
    ws_nodes.append(["来源主文件", source_workbook])
    ws_nodes.append(["导出时间", datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
    ws_nodes.append([])

    headers = ["node_id", "parent_id", "label", "source_code", "source_name"] + years + [
        "auto_text",
        "manual_text",
        "final_text",
        "confirmed",
    ]
    ws_nodes.append(headers)

    node_map = {str(n.get("node_id", "")): n for n in (data.get("nodes", []) or [])}
    for t in (data.get("tree", []) or []):
        nid = str(t.get("node_id", ""))
        n = node_map.get(nid, {})
        row = [
            nid,
            str(t.get("parent_id", "")),
            str(t.get("label", "")),
            str(n.get("source_code", "")),
            str(n.get("source_name", "")),
        ]
        vals = n.get("values", {}) if isinstance(n.get("values"), dict) else {}
        for y in years:
            row.append(vals.get(y))
        row.extend(
            [
                str(n.get("auto_text", "")),
                str(n.get("manual_text", "")),
                str(n.get("final_text", "")),
                bool(n.get("confirmed", False)),
            ]
        )
        ws_nodes.append(row)

    ws_tree = wb.create_sheet("收入分析_树结构")
    ws_tree.append(["node_id", "parent_id", "label"])
    for t in (data.get("tree", []) or []):
        ws_tree.append([str(t.get("node_id", "")), str(t.get("parent_id", "")), str(t.get("label", ""))])

    ws_notes = wb.create_sheet("收入分析_汇总说明")
    ws_notes.append(["节点", "说明"])
    ws_notes.append(["3.1", "经常性收益小计（按规则公式计算）"])
    ws_notes.append(["3.2", "非经常性收益小计（按规则公式计算）"])
    ws_notes.append(["3.3", "收益结构判断（按规则公式计算）"])

    for ws in wb.worksheets:
        for col in ws.columns:
            letter = col[0].column_letter
            max_len = max(len(str(c.value or "")) for c in col[:200])
            ws.column_dimensions[letter].width = min(max(12, max_len + 2), 80)

    bio = BytesIO()
    wb.save(bio)
    return bio.getvalue()


def render_sheet_page(group: Dict[str, Any], project_id: str) -> str:
    label = group["label"]
    gid = group["id"]
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{label}</title>
  <style>
    :root {{ --bg:#f6f7f9; --ink:#111827; --line:#cfd5dc; --card:#fff; --acc:#0f766e; --muted:#6b7280; }}
    body {{ margin:0; font-family: "Microsoft YaHei", "PingFang SC", sans-serif; background:var(--bg); color:var(--ink); }}
    .top {{ position:sticky; top:0; background:#fff; border-bottom:1px solid var(--line); padding:10px 16px; display:flex; gap:10px; align-items:center; flex-wrap:wrap; }}
    .btn {{ border:1px solid var(--line); background:#fff; padding:6px 10px; border-radius:8px; cursor:pointer; text-decoration:none; color:inherit; }}
    .btn.primary {{ background:var(--acc); color:#fff; border-color:var(--acc); }}
    .muted {{ color:var(--muted); font-size:12px; }}
    .wrap {{ padding:12px 16px 28px; }}
    .table-wrap {{ overflow:auto; border:1px solid var(--line); background:var(--card); border-radius:10px; }}
    table {{ border-collapse:collapse; width:100%; min-width:1250px; }}
    th,td {{ border-bottom:1px solid #e5e7eb; padding:8px; vertical-align:top; font-size:13px; }}
    th {{ position:sticky; top:0; background:#f9fafb; z-index:1; text-align:left; }}
    td.num {{ text-align:right; white-space:nowrap; }}
    textarea {{ width:100%; min-height:76px; resize:vertical; font:inherit; }}
    .pill {{ display:inline-block; border:1px solid var(--line); border-radius:999px; padding:2px 8px; font-size:12px; }}
    .dirty {{ background:#fff7ed; }}
  </style>
</head>
<body>
  <div class="top">
    <a href="/?project_id={project_id}" class="btn">返回主页</a>
    <strong>{label}</strong>
    <span class="muted" id="status"></span>
    <span class="muted">项目ID: {project_id}</span>
    <button class="btn" onclick="reloadData()">刷新</button>
    <button class="btn primary" id="saveBtn" onclick="saveAll()">保存全部修改</button>
  </div>
  <div class="wrap">
    <div class="table-wrap">
      <table id="tbl">
        <thead id="thead"></thead>
        <tbody id="tbody"></tbody>
      </table>
    </div>
  </div>
<script>
const PROJECT_ID = {json.dumps(project_id)};
const GROUP_ID = {json.dumps(gid)};
let cache = null;
if (GROUP_ID === 'bs') {{
  const btn = document.getElementById('saveBtn');
  if (btn) btn.textContent = '保存确认';
}}

function esc(s) {{
  return (s ?? '').toString().replace(/[&<>"]/g, c => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}}[c]));
}}

function markDirty(el) {{
  el.closest('tr').classList.add('dirty');
}}

async function reloadData() {{
  const res = await fetch(`/api/sheet/${{GROUP_ID}}?project_id=${{encodeURIComponent(PROJECT_ID)}}`);
  cache = await res.json();
  if (!res.ok) {{
    document.getElementById('status').textContent = cache.error || '读取失败';
    document.getElementById('thead').innerHTML = '';
    document.getElementById('tbody').innerHTML = '';
    return;
  }}
  render();
}}

function render() {{
  const years = cache.years || [];
  document.getElementById('status').textContent = `行数 ${{cache.rows.length}}` + (cache.sheet_title ? ` | 来源Sheet: ${{cache.sheet_title}}` : '');
  const head = GROUP_ID === 'bs'
    ? `<tr><th style="min-width:90px;">科目编码</th><th style="min-width:180px;">科目</th>${{years.map(y=>`<th>${{y}}</th>`).join('')}}<th style="min-width:460px;">分析判断内容</th><th>确认</th></tr>`
    : `<tr><th style="min-width:90px;">科目编码</th><th style="min-width:180px;">科目</th>${{years.map(y=>`<th>${{y}}</th>`).join('')}}<th style="min-width:280px;">自动描述</th><th style="min-width:340px;">人工确认描述</th><th>确认</th></tr>`;
  document.getElementById('thead').innerHTML = head;

  const rowsHtml = cache.rows.map((r, idx) => {{
    const vals = years.map(y => `<td class="num">${{r.values[y] == null ? '' : Number(r.values[y]).toLocaleString(undefined, {{minimumFractionDigits:2, maximumFractionDigits:2}})}}</td>`).join('');
    if (GROUP_ID === 'bs') {{
      return `<tr data-i="${{idx}}">
        <td>${{esc(r.code)}}</td>
        <td>${{esc(r.name)}}</td>
        ${{vals}}
        <td>${{esc(r.analysis_text || '')}}</td>
        <td><label class="pill"><input type="checkbox" ${{r.confirmed ? 'checked' : ''}} onchange="markDirty(this)">确认</label></td>
      </tr>`;
    }}
    return `<tr data-i="${{idx}}">
      <td>${{esc(r.code)}}</td>
      <td>${{esc(r.name)}}</td>
      ${{vals}}
      <td>${{esc(r.auto_text || '')}}</td>
      <td><textarea oninput="markDirty(this)">${{esc(r.manual_text || '')}}</textarea></td>
      <td><label class="pill"><input type="checkbox" ${{r.confirmed ? 'checked' : ''}} onchange="markDirty(this)">确认</label></td>
    </tr>`;
  }}).join('');
  document.getElementById('tbody').innerHTML = rowsHtml;
}}

async function saveAll() {{
  if (!cache) return;
  const years = cache.years || [];
  const rows = [...document.querySelectorAll('#tbody tr')].map(tr => {{
    const i = Number(tr.dataset.i);
    const base = cache.rows[i];
    const confirmed = tr.querySelector('input[type="checkbox"]').checked;
    const txt = GROUP_ID === 'bs' ? '' : tr.querySelector('textarea').value;
    return {{
      code: base.code,
      name: base.name,
      years,
      values: base.values,
      auto_text: base.auto_text,
      manual_text: txt,
      confirmed
    }};
  }});

  const res = await fetch('/api/save', {{
    method: 'POST',
    headers: {{'Content-Type':'application/json'}},
    body: JSON.stringify({{project_id: PROJECT_ID, group_id: GROUP_ID, rows}})
  }});
  const data = await res.json();
  document.getElementById('status').textContent = data.ok ? `已保存 ${{data.saved_rows}} 行` : (`保存失败: ` + (data.error || 'unknown'));
  if (data.ok) {{
    document.querySelectorAll('#tbody tr').forEach(x => x.classList.remove('dirty'));
  }}
}}

reloadData();
</script>
</body>
</html>"""


class AppHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args):  # noqa: A003
        return

    def _send_json(self, obj: Dict[str, Any], status: int = 200) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html: str, status: int = 200) -> None:
        body = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_xlsx(self, body: bytes, filename: str, status: int = 200) -> None:
        safe_name = filename or "export.xlsx"
        self.send_response(status)
        self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Disposition", f"attachment; filename*=UTF-8''{quote(safe_name)}")
        self.end_headers()
        self.wfile.write(body)

    def _project_id(self, qs: Dict[str, List[str]]) -> str:
        return normalize_project_id((qs.get("project_id") or [DEFAULT_PROJECT_ID])[0])

    def do_GET(self):  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path == "/":
            self._send_html(render_index())
            return

        if path.startswith("/sheet/"):
            group_id = unquote(path.split("/")[-1]).strip()
            group = next((g for g in SHEET_GROUPS if g["id"] == group_id), None)
            if not group:
                self._send_html("<h1>Unknown sheet</h1>", status=404)
                return
            self._send_html(render_sheet_page(group, self._project_id(qs)))
            return

        if path == "/analysis/assets":
            self._send_html(render_asset_analysis_page(self._project_id(qs)))
            return

        if path == "/analysis/liabilities":
            self._send_html(render_liability_analysis_page(self._project_id(qs)))
            return

        if path == "/analysis/income":
            self._send_html(render_income_analysis_page(self._project_id(qs)))
            return

        if path == "/analysis/ratios":
            self._send_html(render_ratio_analysis_page(self._project_id(qs)))
            return

        if path == "/analysis/key-ratios":
            self._send_html(render_key_ratio_analysis_page(self._project_id(qs)))
            return

        if path.startswith("/api/sheet/"):
            group_id = unquote(path.split("/")[-1]).strip()
            project_id = self._project_id(qs)
            group = next((g for g in SHEET_GROUPS if g["id"] == group_id), None)
            if not group:
                self._send_json({"error": "unknown group"}, status=404)
                return

            wb_path = workbook_path(project_id)
            if not wb_path.exists():
                self._send_json({"error": f"workbook not found: {wb_path}"}, status=404)
                return

            wb = load_workbook(wb_path, data_only=False)
            sheet_data = read_sheet_rows(wb, group)
            years = sheet_data["years"]
            rows = sheet_data["rows"]
            asset_analysis_map = build_asset_analysis_map(wb, project_id) if group_id == "bs" else {}
            liability_analysis_map = build_liability_analysis_map(wb, project_id) if group_id == "bs" else {}

            store = load_store(project_id)
            entries = store.get("entries", {})
            for r in rows:
                key = make_entry_key(group_id, r["code"])
                saved = entries.get(key, {})
                r["auto_text"] = generate_auto_text(r["name"], r["values"], years)
                r["manual_text"] = str(saved.get("manual_text", "") or "")
                r["confirmed"] = bool(saved.get("confirmed", False))
                if group_id == "bs":
                    code = str(r["code"])
                    a = asset_analysis_map.get(code) or liability_analysis_map.get(code) or {}
                    r["analysis_text"] = str(a.get("final_text", "") or "")

            self._send_json(
                {
                    "project_id": project_id,
                    "group_id": group_id,
                    "group_label": group["label"],
                    "sheet_title": sheet_data["sheet_title"],
                    "years": years,
                    "rows": rows,
                    "workbook": str(wb_path),
                }
            )
            return

        if path == "/api/analysis/assets":
            project_id = self._project_id(qs)
            wb_path = workbook_path(project_id)
            if not wb_path.exists():
                self._send_json({"error": f"workbook not found: {wb_path}"}, status=404)
                return
            wb = load_workbook(wb_path, data_only=False)
            analysis = read_analysis_data(wb, ASSET_ANALYSIS_SHEETS)
            analysis = apply_analysis_code_redirects("asset_analysis", analysis)
            analysis_map = build_asset_analysis_map(wb, project_id)
            self._send_json(
                {
                    "project_id": project_id,
                    "workbook": str(wb_path),
                    "scale": analysis.get("scale", {}),
                    "structure": analysis.get("structure", {}),
                    "analysis": {
                        "rows": list(analysis_map.values()),
                    },
                }
            )
            return

        if path == "/api/analysis/liabilities":
            project_id = self._project_id(qs)
            wb_path = workbook_path(project_id)
            if not wb_path.exists():
                self._send_json({"error": f"workbook not found: {wb_path}"}, status=404)
                return
            wb = load_workbook(wb_path, data_only=False)
            analysis = read_analysis_data(wb, LIABILITY_ANALYSIS_SHEETS)
            analysis = apply_analysis_code_redirects("liability_analysis", analysis)
            analysis_map = build_liability_analysis_map(wb, project_id)
            self._send_json(
                {
                    "project_id": project_id,
                    "workbook": str(wb_path),
                    "scale": analysis.get("scale", {}),
                    "structure": analysis.get("structure", {}),
                    "analysis": {
                        "rows": list(analysis_map.values()),
                    },
                }
            )
            return

        if path == "/api/analysis/income":
            project_id = self._project_id(qs)
            wb_path = workbook_path(project_id)
            if not wb_path.exists():
                self._send_json({"error": f"workbook not found: {wb_path}"}, status=404)
                return
            wb = load_workbook(wb_path, data_only=False)
            data = build_income_analysis_map(wb, project_id)
            self._send_json(
                {
                    "project_id": project_id,
                    "workbook": str(wb_path),
                    "sheet_title": data.get("sheet_title"),
                    "years": data.get("years", []),
                    "tree": data.get("tree", []),
                    "nodes": data.get("nodes", []),
                }
            )
            return

        if path == "/api/analysis/ratios":
            project_id = self._project_id(qs)
            wb_path = workbook_path(project_id)
            if not wb_path.exists():
                self._send_json({"error": f"workbook not found: {wb_path}"}, status=404)
                return
            wb = load_workbook(wb_path, data_only=False)
            data = build_ratio_analysis_map(wb, project_id)
            self._send_json(
                {
                    "project_id": project_id,
                    "workbook": str(wb_path),
                    "sheet_title": data.get("sheet_title"),
                    "years": data.get("years", []),
                    "tree": data.get("tree", []),
                    "nodes": data.get("nodes", []),
                }
            )
            return

        if path == "/api/analysis/key-ratios":
            project_id = self._project_id(qs)
            wb_path = workbook_path(project_id)
            if not wb_path.exists():
                self._send_json({"error": f"workbook not found: {wb_path}"}, status=404)
                return
            wb = load_workbook(wb_path, data_only=False)
            data = build_key_ratio_analysis_map(wb, project_id)
            self._send_json(
                {
                    "project_id": project_id,
                    "workbook": str(wb_path),
                    "sheet_title": data.get("sheet_title"),
                    "years": data.get("years", []),
                    "tree": data.get("tree", []),
                    "nodes": data.get("nodes", []),
                }
            )
            return

        if path == "/api/analysis/income/export":
            project_id = self._project_id(qs)
            wb_path = workbook_path(project_id)
            if not wb_path.exists():
                self._send_json({"error": f"workbook not found: {wb_path}"}, status=404)
                return
            wb = load_workbook(wb_path, data_only=False)
            data = build_income_analysis_map(wb, project_id)
            body = build_income_analysis_export_xlsx(data, project_id=project_id, source_workbook=str(wb_path))
            self._send_xlsx(body, f"{project_id}_收入结构分析计算表.xlsx")
            return

        if path == "/api/projects":
            self._send_json({"projects": list_projects()})
            return

        self._send_json({"error": "not found"}, status=404)

    def do_POST(self):  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path != "/api/save":
            self._send_json({"error": "not found"}, status=404)
            return

        try:
            size = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(size).decode("utf-8")
            body = json.loads(raw)
        except Exception:
            self._send_json({"ok": False, "error": "invalid payload"}, status=400)
            return

        project_id = normalize_project_id(str(body.get("project_id", DEFAULT_PROJECT_ID)))
        group_id = str(body.get("group_id", "")).strip()
        rows = body.get("rows", [])
        if not group_id or not isinstance(rows, list):
            self._send_json({"ok": False, "error": "missing group_id or rows"}, status=400)
            return

        store = load_store(project_id)
        entries = store.setdefault("entries", {})
        now = datetime.now().isoformat(timespec="seconds")
        saved = 0

        for r in rows:
            code = str(r.get("code", "")).strip()
            if not code:
                continue
            key = make_entry_key(group_id, code)
            entries[key] = {
                "code": code,
                "name": str(r.get("name", "")).strip(),
                "manual_text": str(r.get("manual_text", "")).strip(),
                "confirmed": bool(r.get("confirmed", False)),
                "updated_at": now,
            }
            saved += 1

        save_store(project_id, store)
        self._send_json({"ok": True, "saved_rows": saved, "updated_at": now})


def main() -> int:
    parser = argparse.ArgumentParser(description="Local workbook web UI")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), AppHandler)
    print(f"Web UI running: http://{args.host}:{args.port}")
    print("Use query param project_id if needed, e.g. /sheet/bs?project_id=zhonggang_luonai")
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
