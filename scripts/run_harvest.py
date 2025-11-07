# scripts/run_harvest.py
import os, sys
from datetime import date

# ---- src/ import shim (so you don't need pip install -e . yet)
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)
# -------------------------------------------------------------

from preprint_harvester.harvest_crossref import harvest_preprints_dataframe

# Edit these as needed (NO trailing commas!)
date_start = "2025-10-01"                 # must be 'YYYY-MM-DD' string
date_end   = date.today().isoformat()     # string like '2025-11-07'
mailto     = "YOU@org.edu"                # <-- put a real email

if __name__ == "__main__":
    df = harvest_preprints_dataframe(
        date_start=date_start,
        date_end=date_end,
        mailto=mailto,
        prefixes=None,                    # e.g. ["10.1101", "10.48550", ...]
        batch_days=7,
        rows_per_call=1000,
        sort_key="deposited",
        # Keep final saves None unless you've refactored harvester save logic:
        save_parquet_path=None,
        save_csv_path=None,
        save_pkl_path=None,
        # save_ndjson_path="data/crossref_preprints_wide.ndjson.gz",
        adaptive_threshold=2000,
        min_window_seconds=1800,
        incremental_save_dir=f"data/runs/crossref_batches_{date_start}_to_{date_end}",
        doi_refetch_enabled=True,
        doi_refetch_threshold=100000,
        doi_refetch_sleep_s=0.05,
    )
    print("Final shape:", df.shape)
