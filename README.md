# Credit Risk System (MVP)

This project scaffolds a full pipeline for enterprise credit risk analysis based on financial reports.
It borrows proven ideas from:
- revenue_analysis_project: structured report generation + qualitative mapping via config
- easyfinancialstatements: standardized indicator system (18 KGI) + clear model definitions

See docs/framework.md for the detailed design.

## Two-Stage Basic Data Workflow

- Stage 1 (initialize once, do not overwrite existing master workbook):
  - `python scripts/init_basic_workbook.py --project-id your_project_id`
- Manual edit:
  - update `outputs/your_project_id/基础数据_主文件.xlsx`
- Stage 2 (repeatable validation only):
  - `python scripts/validate_basic_workbook.py --project-id your_project_id`

Input file convention:
- put audit PDFs and source Excels under `inputs/<project_id>/` (recommended), file names can be arbitrary
- auto-detection scans recursively under `inputs/<project_id>/` first, then `inputs/`
- discovery priority focuses on three key groups: `报表` / `审计报告` / `评级报告`
- if needed, pass explicit source files via `scripts/build_basic_data_workbook.py --workpaper-path ... --bs-path ...`
- discovery result is saved to `data/<project_id>/input_discovery.json`

Output convention:
- each project is isolated under `outputs/<project_id>/`, `data/<project_id>/`, and `logs/<project_id>/`

Validation stage refreshes:
- `勾稽校验`
- `财务比率` (driven by `config/financial_ratio_rules_v1.json`)
- `差异缺失清单`

and does not rebuild or overwrite statement data sheets.
