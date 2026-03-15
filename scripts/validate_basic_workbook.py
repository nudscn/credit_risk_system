#!/usr/bin/env python3
"""Stage 2 validator for the single master workbook.

Behavior:
- Reads the manually edited master workbook.
- Rebuilds reconciliation sheet.
- Rebuilds issue list (missing values + reconciliation failures).
- Never rebuilds templates or auto-filled statement values.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from openpyxl import load_workbook

from build_basic_data_workbook import (
    build_missing_sheet,
    build_ratio_sheet,
    build_recon_sheet,
    load_ratio_cfg,
    load_recon_cfg,
)

PROJECT_ROOT = Path("/Users/sijia/Documents/credit_risk_system")
MASTER_WORKBOOK = PROJECT_ROOT / "outputs" / "十堰城运_基础数据_主文件.xlsx"

STATEMENT_SHEETS = [
    "资产负债表",
    "利润表",
    "现金流量表",
    "所有者权益变动表",
    "现金流补充资料",
]


def parse_years(ws) -> List[str]:
    years: List[str] = []
    for cell in ws[1]:
        text = str(cell.value or "")
        # Only parse value columns like "2024年（万元）", ignore status columns.
        m = re.search(r"(\d{4})年（万元）", text)
        if m:
            years.append(m.group(1))
    # Keep order and de-duplicate.
    uniq: List[str] = []
    for y in years:
        if y not in uniq:
            uniq.append(y)
    return uniq


def normalize_num(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "")
    if s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def extract_statement_values(ws, years: List[str]) -> Dict[Tuple[str, str], Optional[float]]:
    values: Dict[Tuple[str, str], Optional[float]] = {}
    for row in ws.iter_rows(min_row=2, values_only=True):
        name = row[1]
        if not name:
            continue
        for i, y in enumerate(years):
            val = normalize_num(row[2 + i])
            values[(str(name), y)] = val
    return values


def collect_missing_rows(ws, years: List[str]) -> List[dict]:
    rows: List[dict] = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        code = row[0]
        name = row[1]
        if not name:
            continue
        for i, y in enumerate(years):
            val = normalize_num(row[2 + i])
            if val is None:
                rows.append(
                    {
                        "报表": ws.title,
                        "科目编码": code,
                        "科目名称": name,
                        "期间": y,
                        "状态": "待补充",
                        "说明": "主文件该字段为空，请手工补录或修正",
                    }
                )
    return rows


def collect_recon_issues(wb) -> List[dict]:
    issues: List[dict] = []
    ws = wb["勾稽校验"]
    for row in ws.iter_rows(min_row=2, values_only=True):
        rule_id, desc, period, left, right, diff, result = row
        if result == "是":
            continue
        issues.append(
            {
                "报表": "勾稽校验",
                "科目编码": rule_id,
                "科目名称": desc,
                "期间": period,
                "状态": result or "待补充",
                "说明": f"左值={left}, 右值={right}, 差异={diff}",
            }
        )
    return issues


def create_interest_debt_sheet_if_missing(wb, years: List[str], bs_values: Dict[Tuple[str, str], Optional[float]]) -> None:
    if "有息负债明细" in wb.sheetnames:
        return
    ws = wb.create_sheet("有息负债明细")
    ws.append(["期间", "子项", "值(万元)", "说明"])
    items = ["短期借款", "一年内到期的非流动负债", "长期借款", "应付债券", "租赁负债"]
    for y in years:
        total = 0.0
        has_val = False
        for item in items:
            v = bs_values.get((item, y))
            if v is not None:
                total += v
                has_val = True
            ws.append([y, item, v, "根据资产负债表自动初始化"])
        ws.append([y, "有息负债合计", round(total, 2) if has_val else None, "根据资产负债表自动初始化"])


def extract_interest_debt_values(wb, years: List[str]) -> Tuple[Dict[Tuple[str, str], Optional[float]], List[dict]]:
    values: Dict[Tuple[str, str], Optional[float]] = {}
    missing_rows: List[dict] = []
    if "有息负债明细" not in wb.sheetnames:
        return values, missing_rows

    ws = wb["有息负债明细"]
    # Expected columns: 期间, 子项, 值(万元), 说明
    for row in ws.iter_rows(min_row=2, values_only=True):
        period, item, val, _ = row
        if str(item or "") != "有息负债合计":
            continue
        y = str(period or "")
        if y not in years:
            continue
        n = normalize_num(val)
        values[("有息负债合计", y)] = n
        if n is None:
            missing_rows.append(
                {
                    "报表": "有息负债明细",
                    "科目编码": "DEBT999",
                    "科目名称": "有息负债合计",
                    "期间": y,
                    "状态": "待补充",
                    "说明": "有息负债合计为空，请补录或修正",
                }
            )
    return values, missing_rows


def main() -> int:
    if not MASTER_WORKBOOK.exists():
        print(f"Master workbook not found: {MASTER_WORKBOOK}")
        print("Please run stage 1 first: scripts/init_basic_workbook.py")
        return 1

    wb = load_workbook(MASTER_WORKBOOK)
    if "资产负债表" not in wb.sheetnames:
        print("Master workbook is invalid: missing 资产负债表 sheet")
        return 1

    years = parse_years(wb["资产负债表"])
    if not years:
        print("Cannot detect years from 资产负债表 header")
        return 1

    all_values: Dict[str, Dict[Tuple[str, str], Optional[float]]] = {}
    missing_rows: List[dict] = []

    for sname in STATEMENT_SHEETS:
        if sname not in wb.sheetnames:
            continue
        ws = wb[sname]
        all_values[sname] = extract_statement_values(ws, years)
        missing_rows.extend(collect_missing_rows(ws, years))

    create_interest_debt_sheet_if_missing(wb, years, all_values.get("资产负债表", {}))
    debt_values, debt_missing = extract_interest_debt_values(wb, years)
    if debt_values:
        all_values["有息负债明细"] = debt_values
    missing_rows.extend(debt_missing)

    for name in ["勾稽校验", "财务比率", "差异缺失清单"]:
        if name in wb.sheetnames:
            del wb[name]

    recon_cfg = load_recon_cfg()
    ratio_cfg = load_ratio_cfg()
    build_recon_sheet(wb, years, all_values, recon_cfg)
    build_ratio_sheet(wb, years, all_values, ratio_cfg)

    missing_rows.extend(collect_recon_issues(wb))
    build_missing_sheet(wb, missing_rows)

    first_order = [
        "资产负债表",
        "利润表",
        "现金流量表",
        "所有者权益变动表",
        "现金流补充资料",
        "有息负债明细",
        "勾稽校验",
        "财务比率",
        "差异缺失清单",
    ]
    wb._sheets.sort(key=lambda ws: first_order.index(ws.title) if ws.title in first_order else 100)

    wb.save(MASTER_WORKBOOK)

    recon_yes = recon_no = recon_pending = 0
    for row in wb["勾稽校验"].iter_rows(min_row=2, values_only=True):
        r = row[6]
        if r == "是":
            recon_yes += 1
        elif r == "否":
            recon_no += 1
        else:
            recon_pending += 1

    print(f"Validated: {MASTER_WORKBOOK}")
    print(f"Years: {', '.join(years)}")
    print(f"Recon summary: 是={recon_yes}, 否={recon_no}, 待补充={recon_pending}")
    print(f"Issue rows: {len(missing_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
