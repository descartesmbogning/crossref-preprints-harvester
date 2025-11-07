# scripts/merge_runs.py
import os, sys

# ---- src/ import shim (same as run_harvest.py)
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)
# ---------------------------------------------------

from preprint_harvester.merge_parquets import merge_parquets_and_report

if __name__ == "__main__":
    # Update with the actual folders where your batches are stored
    INPUT_PARENT_DIRS = [
        "data/runs",                       # root folder with all runs
        # or paths like:
        # "data/runs/crossref_batches_2015-01-01_to_2022-12-31",
        # "data/runs/crossref_batches_2023-01-01_to_2024-12-31",
    ]

    OUTPUT_DIR = "data/merged"
    BASENAME = "crossref_preprints_merged"

    outputs, report_df = merge_parquets_and_report(
        input_parent_dirs=INPUT_PARENT_DIRS,
        output_dir=OUTPUT_DIR,
        merged_basename=BASENAME,
        write_pkl_too=False
    )

    print("✅ Merge completed")
    print(outputs)
