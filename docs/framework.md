# Framework (MVP)

## Goal
Build a full pipeline for enterprise credit risk analysis based on financial reports.
MVP is complete but simplified: 3 audit reports, full subject mapping, core validation, basic audit-style analysis, risk report output.

## Layers
1. Data Ingestion
   - Input: audit report PDFs (2022/2023/2024)
   - Output: structured intermediate table with page/coordinates/confidence

2. Subject Mapping (Full Coverage)
   - All report items mapped to a standard subject system
   - Alias/abbreviation support
   - Unmatched items are tracked as issues

3. Data Quality & Reconciliation
   - Consistency checks with tolerance
   - Rule families: balance sheet structure, subtotal rollups, period roll-forward
   - Output: variance list with source/page

4. Audit-style Analysis (Simplified)
   - 3-year trend and structure changes
   - 18 KGI indicator set (expandable)
   - Anomaly list (volatility, reversal, mismatch)

5. Risk Output
   - Base risk report template
   - Required variance disclosure
   - Risk grade + evidence summary

## Rule Governance
Rules are configured in /rules and versioned. All deviations are logged to /logs.
