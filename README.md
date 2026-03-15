# Credit Risk System (MVP)

This project scaffolds a full pipeline for enterprise credit risk analysis based on financial reports.
It borrows proven ideas from:
- revenue_analysis_project: structured report generation + qualitative mapping via config
- easyfinancialstatements: standardized indicator system (18 KGI) + clear model definitions

See docs/framework.md for the detailed design.

## Two-Stage Basic Data Workflow

- Stage 1 (initialize once, do not overwrite existing master workbook):
  - `/Users/sijia/.venv/venv_mac/bin/python /Users/sijia/Documents/credit_risk_system/scripts/init_basic_workbook.py`
- Manual edit:
  - update `/Users/sijia/Documents/credit_risk_system/outputs/十堰城运_基础数据_主文件.xlsx`
- Stage 2 (repeatable validation only):
  - `/Users/sijia/.venv/venv_mac/bin/python /Users/sijia/Documents/credit_risk_system/scripts/validate_basic_workbook.py`

Validation stage refreshes:
- `勾稽校验`
- `财务比率` (driven by `config/financial_ratio_rules_v1.json`)
- `差异缺失清单`

and does not rebuild or overwrite statement data sheets.
