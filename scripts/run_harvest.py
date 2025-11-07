
from src.preprint_harvester.harvest_crossref import harvest_preprints_dataframe

if __name__ == "__main__":
    # EDIT these parameters as needed
    df = harvest_preprints_dataframe(
        date_start="2015-01-01",
        date_end=None,  # defaults to today
        mailto="YOUR_EMAIL@example.com",  # <-- put a real email
        prefixes=None,                # e.g. ["10.1101","10.48550","10.31219","10.2139"]
        batch_days=7,
        rows_per_call=1000,
        sort_key="deposited",
        save_parquet_path="crossref_preprints_wide.parquet",
        save_csv_path="crossref_preprints_wide.csv",
        save_pkl_path="crossref_preprints_wide.pkl",
        # save_ndjson_path="crossref_preprints_wide.ndjson.gz",
        adaptive_threshold=2000,
        min_window_seconds=1800,
        incremental_save_dir="crossref_batches_2015-01-01_to_today",
        doi_refetch_enabled=True,
        doi_refetch_threshold=1800,
        doi_refetch_sleep_s=0.05,
    )
    print("Final shape:", df.shape)
