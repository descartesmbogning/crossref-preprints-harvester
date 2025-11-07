
#!/usr/bin/env python3
"""
merge_parquets.py
=================
Merge multiple Crossref preprint Parquet shards into a single dataset,
aligning schemas and producing both a merged Parquet/CSV and an audit report.

Highlights
----------
- Robust discovery across one-level-deep subfolders (optional recursive)
- Schema unification (Arrow) with safe type casting
- Stream-append CSV in chunks to manage memory
- Atomic Parquet finalize + Markdown/CSV audit report
- Pure local paths: **no Colab / Google Drive assumptions**

Usage
-----
pip install pandas pyarrow

python merge_parquets.py   --inputs ./data/batches/run_2025_10_10 ./data/batches/run_2025_10_17   --output-dir ./data/output   --basename crossref_preprints_merged   --name-contains ""   --one-level-deep   --write-pkl 0

Notes
-----
- Use --recursive to search all nested subfolders instead of one-level-deep.
- Use --name-contains to include only files whose filename contains a substring.
"""

import os
import re
import glob
import time
import argparse
from datetime import datetime
from typing import List, Tuple, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

CSV_WRITE_CHUNK_ROWS = 250_000  # CSV rows per append chunk

def _norm(p: str) -> str:
    return os.path.normpath(os.path.expanduser(p))

DATE_RANGE_REGEXES = [
    re.compile(r'(?P<start>\d{4}-\d{2}-\d{2}).{0,5}(?P<end>\d{4}-\d{2}-\d{2})'),
    re.compile(r'from[_\-]?(?P<start>\d{4}-\d{2}-\d{2}).{0,5}until[_\-]?(?P<end>\d{4}-\d{2}-\d{2})'),
]

def extract_date_range_from_name(name: str) -> Tuple[Optional[str], Optional[str]]:
    base = os.path.basename(name)
    for rx in DATE_RANGE_REGEXES:
        m = rx.search(base)
        if m:
            return m.group('start'), m.group('end')
    for rx in DATE_RANGE_REGEXES:
        m = rx.search(name)
        if m:
            return m.group('start'), m.group('end')
    return None, None

def discover_parquet_files(parents: List[str], name_contains: str = "", one_level_deep: bool = True, recursive: bool = False) -> List[dict]:
    records = []
    for parent in parents:
        parent = _norm(parent)
        if not os.path.exists(parent):
            print(f"[warn] Missing parent dir: {parent}")
            continue

        if recursive:
            folders = [parent]
        elif one_level_deep:
            folders = [os.path.join(parent, d) for d in os.listdir(parent)
                       if os.path.isdir(os.path.join(parent, d))]
            folders.append(parent)  # include parent itself
        else:
            folders = [parent]

        seen = set()
        for folder in folders:
            pattern = os.path.join(folder, '**', '*.parquet') if recursive else os.path.join(folder, '*.parquet')
            for fpath in glob.iglob(pattern, recursive=recursive):
                fpath = _norm(fpath)
                if fpath in seen:
                    continue
                seen.add(fpath)

                fname = os.path.basename(fpath)
                if name_contains and name_contains not in fname:
                    continue

                try:
                    size_bytes = os.path.getsize(fpath)
                except OSError:
                    size_bytes = None

                num_rows = None
                try:
                    pf = pq.ParquetFile(fpath)
                    num_rows = pf.metadata.num_rows
                except Exception:
                    try:
                        num_rows = len(pd.read_parquet(fpath, columns=['doi']))
                    except Exception:
                        num_rows = None

                start_date, end_date = extract_date_range_from_name(fpath)
                if not start_date and not end_date:
                    start_date, end_date = extract_date_range_from_name(folder)

                records.append({
                    'folder': folder,
                    'filepath': fpath,
                    'filename': fname,
                    'filesize_bytes': size_bytes,
                    'rows': num_rows,
                    'start_date_inferred': start_date,
                    'end_date_inferred': end_date,
                    'suspected_incomplete': (num_rows == 2000) if isinstance(num_rows, int) else None
                })
    return records

def human_size(n: Optional[int]) -> str:
    if n is None:
        return "n/a"
    for unit in ['B','KB','MB','GB','TB']:
        if n < 1024.0:
            return f"{n:3.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PB"

def _collect_unified_schema(paths: List[str]) -> pa.schema:
    schemas = []
    for p in paths:
        try:
            pf = pq.ParquetFile(p)
            schema = pf.schema_arrow
            # Example: normalize 'score' to float64 if inconsistent
            if 'score' in schema.names and not schema.field('score').type.equals(pa.float64()):
                fields = []
                for field in schema:
                    if field.name == 'score':
                        fields.append(pa.field('score', pa.float64()))
                    else:
                        fields.append(field)
                schema = pa.schema(fields)
            schemas.append(schema)
        except Exception as e:
            print(f"[warn] Could not read schema for {p}: {e}")
    return pa.unify_schemas(schemas) if schemas else pa.schema([])

def _align_table_to_schema(tbl: pa.Table, schema: pa.schema) -> pa.Table:
    cols = []
    for f in schema:
        if f.name in tbl.schema.names:
            col = tbl[f.name]
            if not col.type.equals(f.type):
                try:
                    col = pa.compute.cast(col, f.type)
                except Exception:
                    pass
            cols.append(col)
        else:
            cols.append(pa.nulls(length=tbl.num_rows, type=f.type).rename(f.name))
    return pa.table(cols, schema=schema)

def _stream_write_csv(table: pa.Table, out_csv: str, first_write: bool):
    df_chunk = table.to_pandas(types_mapper=pd.ArrowDtype)
    df_chunk.to_csv(out_csv, index=False, header=first_write, mode='w' if first_write else 'a')

def merge_parquets_and_report(
    input_parent_dirs: List[str],
    output_dir: str,
    merged_basename: str = "crossref_preprints_merged",
    name_contains: str = "",
    one_level_deep: bool = True,
    recursive: bool = False,
    write_pkl_too: bool = False,
):
    # Resolve/ensure output directory
    output_dir = _norm(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    # 1) Discover shards
    recs = discover_parquet_files(input_parent_dirs, name_contains=name_contains, one_level_deep=one_level_deep, recursive=recursive)
    if not recs:
        raise RuntimeError("No .parquet files found under the provided parent directories.")
    report_df = pd.DataFrame(recs).sort_values(['folder', 'filename']).reset_index(drop=True)

    # 2) Unified schema
    all_paths = report_df['filepath'].tolist()
    unified_schema = _collect_unified_schema(all_paths)
    if len(unified_schema) == 0:
        raise RuntimeError("Could not determine a unified schema from the shards.")

    # 3) Prepare outputs (temp + final)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_parquet = _norm(os.path.join(output_dir, f"{merged_basename}_{ts}.parquet"))
    out_parquet_tmp = out_parquet + ".tmp"
    out_csv     = _norm(os.path.join(output_dir, f"{merged_basename}_{ts}.csv"))
    out_pkl     = _norm(os.path.join(output_dir, f"{merged_basename}_{ts}.pkl"))
    out_report_csv = _norm(os.path.join(output_dir, f"{merged_basename}_REPORT_{ts}.csv"))
    out_report_md  = _norm(os.path.join(output_dir, f"{merged_basename}_REPORT_{ts}.md"))

    # 4) Stream write Parquet + CSV with safety
    total_rows_written = 0
    first_csv = True
    parquet_ok = True
    writer = None

    # Create parent dirs (again) just in case
    os.makedirs(os.path.dirname(out_parquet_tmp), exist_ok=True)
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    try:
        try:
            writer = pq.ParquetWriter(out_parquet_tmp, schema=unified_schema, use_dictionary=True, compression='snappy')
        except Exception as e:
            parquet_ok = False
            print(f"[warn] Could not open ParquetWriter, will skip Parquet output: {e}")

        for p in all_paths:
            try:
                table = pq.read_table(p)
                table = _align_table_to_schema(table, unified_schema)

                # Parquet
                if parquet_ok and writer is not None:
                    writer.write_table(table)
                total_rows_written += table.num_rows

                # CSV (append in chunks)
                if table.num_rows <= CSV_WRITE_CHUNK_ROWS:
                    _stream_write_csv(table, out_csv, first_write=first_csv)
                    first_csv = False
                else:
                    start = 0
                    while start < table.num_rows:
                        end = min(start + CSV_WRITE_CHUNK_ROWS, table.num_rows)
                        _stream_write_csv(table.slice(start, end - start), out_csv, first_write=first_csv)
                        first_csv = False
                        start = end

            except Exception as e:
                print(f"[warn] Skipping unreadable parquet during merge: {p} ({e})")

    finally:
        if writer is not None:
            writer.close()

    # Finalize Parquet atomically
    if parquet_ok and os.path.exists(out_parquet_tmp):
        try:
            os.replace(out_parquet_tmp, out_parquet)
        except Exception as e:
            print(f"[warn] Could not finalize Parquet file: {e}")
            parquet_ok = False
            try:
                os.remove(out_parquet_tmp)
            except Exception:
                pass

    # 5) Report
    report_df.to_csv(out_report_csv, index=False)
    with open(out_report_md, "w", encoding="utf-8") as fh:
        fh.write(f"# Crossref Preprints Merge Report ({ts})\n\n")
        fh.write(f"**Output folder:** `{output_dir}`  \n")
        fh.write(f"**Merged outputs:**\n")
        if parquet_ok:
            fh.write(f"- Parquet: `{out_parquet}`\n")
        else:
            fh.write(f"- Parquet: *(skipped due to writer/open error)*\n")
        fh.write(f"- CSV: `{out_csv}`\n")
        if write_pkl_too:
            fh.write(f"- PKL: `{out_pkl}`\n")
        fh.write("\n---\n\n")
        fh.write("## Shards discovered\n\n")
        fh.write("| Folder | Filename | Start date (inferred) | End date (inferred) | Rows | File size | Suspected incomplete |\n")
        fh.write("|---|---|---|---|---:|---:|:--:|\n")
        for _, r in report_df.iterrows():
            fh.write(
                f"| `{r['folder']}` | `{r['filename']}` | {r['start_date_inferred'] or ''} | "
                f"{r['end_date_inferred'] or ''} | {r['rows'] if pd.notna(r['rows']) else ''} | "
                f"{human_size(r['filesize_bytes'])} | "
                f"{'✅' if r.get('suspected_incomplete') else ''} |\n"
            )
        n_files = len(report_df)
        total_rows = int(report_df['rows'].dropna().sum()) if report_df['rows'].notna().any() else 0
        n_suspect = int(report_df['suspected_incomplete'].fillna(False).sum())
        fh.write("\n---\n\n")
        fh.write("**Summary**  \n")
        fh.write(f"- Files found: **{n_files}**  \n")
        fh.write(f"- Total rows (sum of shard counts): **{total_rows:,}**  \n")
        fh.write(f"- Rows written to merged outputs: **{total_rows_written:,}**  \n")
        fh.write(f"- Files with exactly 2000 rows (possibly truncated): **{n_suspect}**  \n")
        fh.write("\n> Note: When a shard has **exactly 2000 rows**, it often means the fetch for that slice\n")
        fh.write("> reached an internal cap; treat those entries as **possibly <100% coverage** for that slice.\n")

    # 6) Optional PKL (load merged parquet)
    if write_pkl_too and parquet_ok:
        try:
            merged_df = pd.read_parquet(out_parquet)
            merged_df.to_pickle(out_pkl)
            del merged_df
        except Exception as e:
            print(f"[warn] Could not write PKL: {e}")

    print("✅ Done.")
    if parquet_ok:
        print(f"Merged Parquet: {out_parquet}")
    else:
        print("Merged Parquet: (skipped)")
    print(f"Merged CSV:     {out_csv}")
    if write_pkl_too:
        print(f"Merged PKL:     {out_pkl}")
    print(f"Report CSV:     {out_report_csv}")
    print(f"Report MD:      {out_report_md}")

    return {
        "parquet": out_parquet if parquet_ok else None,
        "csv": out_csv,
        "pkl": out_pkl if (write_pkl_too and parquet_ok) else None,
        "report_csv": out_report_csv,
        "report_md": out_report_md,
        "output_dir": output_dir
    }, report_df

def _parse_args():
    ap = argparse.ArgumentParser(description="Merge Parquet shards and emit an audit report.")
    ap.add_argument("--inputs", nargs="+", required=True, help="One or more parent directories to search for .parquet shards")
    ap.add_argument("--output-dir", default="./data/output", help="Where merged files + reports will be written")
    ap.add_argument("--basename", default="crossref_preprints_merged", help="Base name for output files")
    ap.add_argument("--name-contains", default="", help="Only include files whose *filename* contains this substring")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--one-level-deep", action="store_true", default=True, help="Search one level of subfolders (default)")
    g.add_argument("--recursive", action="store_true", help="Search recursively through all subfolders")
    ap.add_argument("--write-pkl", type=int, default=0, help="Also write a Pickle snapshot (1 = yes, 0 = no)")
    return ap.parse_args()

def main():
    args = _parse_args()
    one_level_deep = True
    recursive = False
    if args.recursive:
        one_level_deep = False
        recursive = True
    elif args.one_level_deep:
        one_level_deep = True
        recursive = False

    merge_parquets_and_report(
        input_parent_dirs=args.inputs,
        output_dir=args.output_dir,
        merged_basename=args.basename,
        name_contains=args.name_contains,
        one_level_deep=one_level_deep,
        recursive=recursive,
        write_pkl_too=bool(args.write_pkl),
    )

if __name__ == "__main__":
    main()
