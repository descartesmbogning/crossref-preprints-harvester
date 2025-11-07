# QA & Pre‑Flight Checklist
_Last updated: 2025-11-07 08:49:10_

Use this list **before** sharing numbers or publishing dashboards.

## 1) Sanity checks
- [ ] Row count > 0 for the requested period
- [ ] No duplicated `doi` after de‑duplication
- [ ] Earliest and latest `posted_date` match the intended range

## 2) Field coverage
- [ ] % non‑null for `title`, `posted_date`, `publisher` looks reasonable
- [ ] Randomly spot‑check 10 DOIs resolve and landing `url` is accessible
- [ ] If `primary_url` is blank, ensure `links_json` or `url` is present

## 3) Server signals
- [ ] Summarize by `prefix` and `publisher`; compare to known servers list (if any)
- [ ] Inspect outliers in `institution_name` and `group_title`

## 4) Relations & versioning
- [ ] % rows with `is_preprint_of` and `is_version_of` populated
- [ ] Randomly validate 5 linkages by resolving the DOIs

## 5) Reproducibility
- [ ] Record parameters in `RUN_SHEET.txt`
- [ ] Archive `.ndjson.gz` stream (if enabled)
- [ ] Save outputs to the team folder with timestamp

## 6) Known limitations
- Crossref metadata varies by depositor; treat server signals as **indicative**, not definitive.
- Very recent records may update as upstream metadata changes.
