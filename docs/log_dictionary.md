# Log Dictionary

## pipeline_events.jsonl fields
- ts: UTC timestamp
- stage: pipeline stage name
- level: info, warn, error
- message: event message
- payload: stage-specific object

## Current stages
- pipeline
- ingest
- mapping
- recon
- analysis
- risk

## Required logging focus
- Data quality issues
- Reconciliation failures
- Cross-source variance notes
- Human override decisions
