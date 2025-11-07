# Crossref Preprints & Posted-Content — Project Report
_Last updated: 2025-11-07 08:49:10_

## Purpose
This document explains, in plain language, how the Crossref preprints harvester works, what the output contains, how to use it, and the safeguards/limitations to keep in mind. It is written for both technical and non‑technical teammates.

## What the harvester does
- **Goal:** Retrieve Crossref **`posted-content`** records (preprints and related items).
- **Output:** A **single wide table** where each row is a record and each column is a clean field.
- **Reliability:** Uses **adaptive window splitting** and a **DOI‑first fallback** to avoid API truncation in dense periods; robust cursor loops and backoff handle transient API issues.

## Key columns (examples)
- `doi`, `title`, `posted_date`, `publisher`, `prefix`, `institution_name`
- `is_preprint_of`, `is_version_of`, `has_preprint` (linkages when provided)
- `primary_url` for explicit full‑text link when `resource.primary.URL` is set
- Rich fields preserved as JSON strings in columns ending with `_json` (e.g., `authors_json`, `licenses_json`, `references_json`)

> See **DATA_DICTIONARY.csv** for the full column list and definitions.

## How non‑developers can use the data
- **Trends:** Group counts by `posted_date` (year/month) to view growth over time.
- **Server signals:** Use `prefix`, `publisher`, `institution_name`, and `group_title` to approximate server‑level views (signals are helpful but not perfect).
- **Versioning:** Use `is_version_of` to estimate multi‑version behavior.
- **Linkage to published articles:** Use `is_preprint_of` when populated.
- **Licensing:** Summarize `license_url` and `licenses_json` for reuse policies.

## Safeguards in the code
- **Adaptive splitting:** Splits big windows automatically until total results are under a safe threshold.
- **DOI‑first fallback:** For very dense windows, fetches DOIs only and then hydrates each DOI to prevent truncation.
- **Cursor hardening:** Handles repeated cursors and zero‑item pages by stepping forward safely.
- **Polite retries:** Retries 429/5xx with exponential backoff and a proper `User-Agent`/`mailto`.

## Known limitations
- **Server labeling is not standardized**: use multiple metadata signals and validate downstream when accuracy is critical.
- **Primary URLs may be absent**: in that case consult `links_json` or `url`.
- **Relations coverage varies** by depositor and over time.

## Recommended workflow
1. Run the harvester and produce CSV/Parquet (and optional NDJSON stream).
2. Fill **RUN_SHEET.txt** with parameters and notes for reproducibility.
3. Complete **QA_CHECKLIST.md** before sharing numbers.
4. Publish/hand off the dataset together with this documentation.

## Glossary
- **Crossref:** DOI registration agency providing scholarly metadata.
- **Posted-content:** Crossref type for preprints and similar non‑peer‑reviewed items.
- **DOI prefix:** Leading part of a DOI (e.g., `10.1101`), often associated with a provider/server.
- **Cursor paging:** Stable method for traversing large result sets in APIs.

**Contact:** _dmbogning15@gmail.com._
