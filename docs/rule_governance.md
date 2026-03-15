# Rule Governance

## Rule Types
1. Ingestion rules
2. Subject mapping rules
3. Reconciliation rules
4. Anomaly rules
5. Risk scoring rules
6. Narrative template rules

## Change Process
1. Update JSON rule file in /rules
2. Increase version field
3. Add note to logs/PROJECT_LOG.md
4. Run pipeline and inspect outputs
5. Keep backward compatibility when possible

## Safety Practices
- Never remove active keys without migration notes
- Keep aliases additive unless confirmed wrong
- Mark high-impact rule changes in project log
