# Project Log

## 2026-03-14
- Initialized project scaffold under /Users/sijia/Documents/credit_risk_system
- Added docs, indicator config, and rule placeholders
- Basis: revenue_analysis_project + easyfinancialstatements (18 KGI)
- Added project config: config/project_config_shiyan.json
- Implemented first rule sets: mapping/recon/anomaly/risk
- Upgraded scripts/run_project.py to executable stage runner
- Executed pipeline successfully and generated outputs + pipeline_events.jsonl
- Added quality stage output: outputs/quality_issues.json
- Added variance stage output: outputs/variance_issues.json
- Added docs/rule_governance.md and docs/log_dictionary.md
- Verified pipeline run after updates (no runtime warnings)
- Added Swift PDFKit fallback extractor (scripts/pdf_extract.swift)
- Upgraded pipeline extraction to use pypdf/PyPDF2 first, then swift_pdfkit fallback
- Added actionable recommendations in quality issues and risk report
- Latest run confirms scan-heavy detection for all three audit reports
- Fixed extraction fallback regression (swift_pdfkit path now continues correctly)
- Added OCR augmentation via scripts/pdf_ocr_extract.swift (swift_vision_ocr)
- Added OCR attempt status fields in extracted file stats
- Improved subject matching with normalized text matching for OCR text
- Latest runs: OCR succeeded on 3/3 PDFs, but extracted candidate volume remains low due scan-heavy quality
- Generated Chinese multi-sheet Excel deliverable with actual values: outputs/十堰城运_组织化财务报表_阶段版.xlsx
- Included sheets: 资产表_组织化, 负债表_组织化, 现金流量表_组织化, 利润表_组织化框架, 填报说明
- Data source for populated values: 分析底稿-十堰城运.xlsx (assets/liabilities/cashflow)
