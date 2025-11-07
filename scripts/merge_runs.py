
from src.preprint_harvester.merge_parquets import merge_parquets_and_report

if __name__ == "__main__":
    INPUT_PARENT_DIRS = [
        "crossref_batches_2015-01-01_to_today",  # add more as needed
    ]
    outputs, report_df = merge_parquets_and_report(
        input_parent_dirs=INPUT_PARENT_DIRS,
        output_dir="crossref_preprints_merged",
        merged_basename="crossref_preprints_merged",
        write_pkl_too=False
    )
    print(outputs)
