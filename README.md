
# Crossref Preprints Harvester

Collects **Crossref `posted-content`** (preprints and related) into a single wide table with robust guards:
adaptive window splitting, hardened cursor loops, and a DOI-first fallback for dense windows.

## What you get
- `crossref_preprints_wide.csv` / `.parquet` / `.pkl`
- Optional `.ndjson.gz` stream for provenance
- Docs in `docs/` (REPORT, DATA_DICTIONARY, QA checklist, Run Sheet)

## Quick start
```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Run a harvest (edit scripts/run_harvest.py if needed)
python scripts/run_harvest.py

# Merge multiple batch folders
python scripts/merge_runs.py
```

## Notes
- Set a real email in `mailto` (Crossref best practice).
- Server labeling is imperfect in Crossref; combine `prefix`, `publisher`, `institution_name`, `group_title`.
- For very large outputs, prefer Parquet or load into a DB.

## License
MIT (see `LICENSE`).
