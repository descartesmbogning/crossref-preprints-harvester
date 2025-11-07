
#!/usr/bin/env python3
"""
harvest_crossref.py
===================
Crossref "posted-content" (preprints & related) harvester that produces a **wide table**
with nested fields serialized to JSON strings.

Key features
------------
- Adaptive time-slice splitting to avoid API truncation
- Hardened cursor loops (retries, repeated-cursor guard, zero-item hop)
- DOI-first fallback hydration for very dense windows
- CLI (argparse) + environment-variable support for mailto
- Outputs CSV/Parquet/PKL/NDJSON to a local folder (default: ./data/output)

Usage
-----
# basic (defaults shown)
python harvest_crossref.py   --date-start 2015-01-01   --date-end TODAY   --mailto you@example.com   --batch-days 7   --rows-per-call 1000   --sort-key deposited   --output-dir ./data/output   --save-csv   --save-parquet   --adaptive-threshold 1500   --min-window-seconds 3600

Environment
-----------
- CROSSREF_MAILTO can be used instead of --mailto.

Dependencies
------------
pip install requests pandas pyarrow tqdm python-dateutil
"""

import os
import sys
import time
import json
import gzip
import argparse
import requests
import pandas as pd
from datetime import date, timedelta, datetime

CROSSREF_WORKS = "https://api.crossref.org/works"
DEFAULT_MAILTO = os.getenv("CROSSREF_MAILTO", "your.email@example.com")
UA = f"Crossref-PreprintHarvester/2.1 (mailto:{DEFAULT_MAILTO})"

try:
    from tqdm import tqdm
    _HAVE_TQDM = True
except Exception:
    _HAVE_TQDM = False

# -------------------- utils --------------------
def _set_mailto(mailto):
    global UA
    UA = f"Crossref-PreprintHarvester/2.1 (mailto:{mailto})"

def _date_from_parts(d):
    try:
        parts = (d or {}).get("date-parts", [[]])[0]
        if not parts: return None
        y = f"{parts[0]:04d}"
        m = f"{(parts[1] if len(parts)>1 else 1):02d}"
        dd = f"{(parts[2] if len(parts)>2 else 1):02d}"
        return f"{y}-{m}-{dd}"
    except Exception:
        return None

def _first(x, key=None):
    if not x: return None
    v = x[0]
    return v.get(key) if key and isinstance(v, dict) else v

def _json(obj):
    if obj is None: return None
    try:
        return json.dumps(obj, ensure_ascii=False, sort_keys=True)
    except Exception:
        return None

def _join_authors(a_list):
    if not a_list: return None
    names = []
    for a in a_list:
        nm = " ".join(filter(None, [a.get("given"), a.get("family")])).strip()
        if not nm:
            nm = a.get("name") or a.get("literal")
        if nm:
            names.append(nm)
    return "; ".join(names) if names else None

def _extract_relations(rel):
    if not rel or not isinstance(rel, dict):
        return (None, None, None)
    def pick(kind):
        items = rel.get(kind) or []
        dois = []
        for it in items:
            doi = it.get("id")
            if doi and doi.lower().startswith("https://doi.org/"):
                doi = doi.split("org/",1)[1]
            if doi:
                dois.append(doi)
        return "; ".join(sorted(set(dois))) if dois else None
    return (pick("is-preprint-of"), pick("has-preprint"), pick("is-version-of"))

# ---------------- HTTP w/ backoff ----------------
def _fetch_page(params, max_retries=6, base_sleep=0.5):
    headers = {"User-Agent": UA}
    last_exc = None
    for attempt in range(max_retries):
        try:
            r = requests.get(CROSSREF_WORKS, params=params, headers=headers, timeout=60)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(base_sleep * (2 ** attempt))
                continue
            try:
                print("Crossref error payload:", r.json())
            except Exception:
                print("Crossref error text:", r.text[:800])
            r.raise_for_status()
        except requests.RequestException as e:
            last_exc = e
            time.sleep(base_sleep * (2 ** attempt))
    if last_exc:
        raise last_exc

# ---------------- total-results probe ----------------
def _total_results(filters, sort_key="deposited"):
    params = {
        "filter": ",".join(filters),
        "rows": 0,
        "cursor": "*",
        "mailto": DEFAULT_MAILTO,
        "sort": sort_key,
        "order": "asc",
    }
    try:
        data = _fetch_page(params)
        return int(data.get("message", {}).get("total-results", 0))
    except Exception:
        params["rows"] = 1
        data = _fetch_page(params)
        return int(data.get("message", {}).get("total-results", 0))

# ---------------- adaptive sub-window splitting ----------------
def _iter_subwindows_adaptive(start_dt, end_dt, prefixes=None, sort_key="deposited",
                              threshold=1500, min_window_seconds=3600):
    def filters_for(a, b):
        return [
            f"from-posted-date:{a.strftime('%Y-%m-%dT%H:%M:%S')}",
            f"until-posted-date:{b.strftime('%Y-%m-%dT%H:%M:%S')}",
            "type:posted-content",
        ]

    filters = filters_for(start_dt, end_dt)
    total = _total_results(filters, sort_key=sort_key)

    if total == 0:
        return

    if total <= threshold or (end_dt - start_dt).total_seconds() <= min_window_seconds:
        yield start_dt.strftime('%Y-%m-%dT%H:%M:%S'), end_dt.strftime('%Y-%m-%dT%H:%M:%S'), prefixes
        return

    mid_ts = start_dt.timestamp() + (end_dt.timestamp() - start_dt.timestamp()) / 2.0
    mid = datetime.fromtimestamp(int(mid_ts))

    yield from _iter_subwindows_adaptive(start_dt, mid, prefixes, sort_key, threshold, min_window_seconds)
    right_start = mid + timedelta(seconds=1)
    yield from _iter_subwindows_adaptive(right_start, end_dt, prefixes, sort_key, threshold, min_window_seconds)

def _prefix_fanout(prefixes):
    if not prefixes: return None
    if isinstance(prefixes, (list, tuple)):
        cleaned = [str(p).strip() for p in prefixes if p]
        return cleaned or None
    return [str(prefixes).strip()]

# ---------------- hardened streaming core ----------------
def _stream_with_filters(filters, rows=1000, cursor="*", sort_key="deposited"):
    assert rows <= 1000, "Crossref caps rows at 1000"
    params = {
        "filter": ",".join(filters),
        "rows": rows,
        "cursor": cursor,
        "mailto": DEFAULT_MAILTO,
        "sort": sort_key,
        "order": "asc",
    }

    total = None
    yielded = 0
    pbar = None
    last_cursors = set()

    while True:
        data = _fetch_page(params)
        msg = data.get("message", {})
        if total is None:
            total = msg.get("total-results") or 0
            if _HAVE_TQDM and isinstance(total, int) and total > 0:
                pbar = tqdm(total=total, desc="Fetching preprints", unit="rec")

        items = msg.get("items") or []
        n = len(items)
        if n == 0:
            cur = msg.get("next-cursor")
            if cur and cur not in last_cursors:
                params["cursor"] = cur
                last_cursors.add(cur)
                continue
            break

        for it in items:
            yielded += 1
            if pbar: pbar.update(1)
            yield it

        if isinstance(total, int) and total > 0 and yielded >= total:
            break

        cur = msg.get("next-cursor")
        if not cur:
            break
        if cur in last_cursors:
            params["cursor"] = cur
            continue
        last_cursors.add(cur)
        params["cursor"] = cur

    if pbar: pbar.close()

def stream_preprints(start_iso, end_iso, mailto=DEFAULT_MAILTO, prefixes=None,
                     rows=1000, cursor="*", sort_key="deposited"):
    _set_mailto(mailto)
    base = [f"from-posted-date:{start_iso}", f"until-posted-date:{end_iso}", "type:posted-content"]
    if prefixes:
        for p in prefixes:
            yield from _stream_with_filters(base + [f"prefix:{p}"], rows=rows, cursor=cursor, sort_key=sort_key)
    else:
        yield from _stream_with_filters(base, rows=rows, cursor=cursor, sort_key=sort_key)

# ---------------- DOI-first helpers ----------------
def _filters_for_window(from_iso, until_iso):
    return [
        f"from-posted-date:{from_iso}",
        f"until-posted-date:{until_iso}",
        "type:posted-content",
    ]

def _filters_with_prefix(base_filters, prefix):
    if prefix:
        return base_filters + [f"prefix:{prefix}"]
    return base_filters

def _stream_doi_list(from_iso, until_iso, mailto=DEFAULT_MAILTO, prefix=None, rows=1000, sort_key="deposited"):
    _set_mailto(mailto)
    base = _filters_for_window(from_iso, until_iso)
    flt = _filters_with_prefix(base, prefix)

    def _stream_doi_only(filters, rows=1000, cursor="*", sort_key="deposited"):
        params = {
            "filter": ",".join(filters),
            "rows": rows,
            "cursor": cursor,
            "mailto": DEFAULT_MAILTO,
            "sort": sort_key,
            "order": "asc",
            "select": "DOI",
        }
        total = None
        yielded = 0
        pbar = None
        last_cursors = set()
        while True:
            data = _fetch_page(params)
            msg = data.get("message", {})
            if total is None:
                total = msg.get("total-results") or 0
                if _HAVE_TQDM and isinstance(total, int) and total > 0:
                    pbar = tqdm(total=total, desc="Fetching preprints", unit="rec")
            items = msg.get("items") or []
            if not items:
                cur = msg.get("next-cursor")
                if cur and cur not in last_cursors:
                    params["cursor"] = cur
                    last_cursors.add(cur)
                    continue
                break
            for it in items:
                doi = it.get("DOI")
                if doi:
                    yielded += 1
                    if pbar: pbar.update(1)
                    yield doi
            if isinstance(total, int) and total > 0 and yielded >= total:
                break
            cur = msg.get("next-cursor")
            if not cur:
                break
            if cur in last_cursors:
                params["cursor"] = cur
                continue
            last_cursors.add(cur)
            params["cursor"] = cur
        if pbar: pbar.close()

    for doi in _stream_doi_only(flt, rows=rows, cursor="*", sort_key=sort_key):
        yield doi

def _fetch_work_by_doi(doi, max_retries=6, base_sleep=0.5):
    headers = {"User-Agent": UA}
    url = f"{CROSSREF_WORKS}/{requests.utils.quote(doi)}"
    last_exc = None
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, timeout=60)
            if r.status_code == 200:
                j = r.json()
                return (j.get("message") or {})
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(base_sleep * (2 ** attempt))
                continue
            try:
                print("Crossref /works/{doi} error payload:", r.json())
            except Exception:
                print("Crossref /works/{doi} error text:", r.text[:800])
            r.raise_for_status()
        except requests.RequestException as e:
            last_exc = e
            time.sleep(base_sleep * (2 ** attempt))
    if last_exc:
        raise last_exc

def _total_results_for_window(from_iso, until_iso, prefix=None, sort_key="deposited"):
    base = _filters_for_window(from_iso, until_iso)
    flt = _filters_with_prefix(base, prefix)
    return _total_results(flt, sort_key=sort_key)

# ---------------- record → wide row ----------------
def _one_row_wide(m):
    title                 = _first(m.get("title"))
    original_title        = _first(m.get("original-title"))
    short_title           = _first(m.get("short-title"))
    subtitle              = _first(m.get("subtitle"))
    container_title       = _first(m.get("container-title"))
    short_container_title = _first(m.get("short-container-title"))

    created_date          = _date_from_parts(m.get("created"))
    posted_date           = _date_from_parts(m.get("posted"))
    deposited_date        = _date_from_parts(m.get("deposited"))
    indexed_date          = _date_from_parts(m.get("indexed"))
    issued_date           = _date_from_parts(m.get("issued"))
    published_online_date = _date_from_parts(m.get("published-online"))
    published_print_date  = _date_from_parts(m.get("published-print"))
    accepted_date         = _date_from_parts(m.get("accepted"))
    approved_date         = _date_from_parts(m.get("approved"))

    is_preprint_of, has_preprint, is_version_of = _extract_relations(m.get("relation"))
    relation_json          = _json(m.get("relation"))

    authors_pretty         = _join_authors(m.get("author"))
    authors_json           = _json(m.get("author"))
    editors_json           = _json(m.get("editor"))
    translators_json       = _json(m.get("translator"))
    chairs_json            = _json(m.get("chair"))
    contributors_json      = _json(m.get("container-contributor") or m.get("contributor"))

    license_url            = _first(m.get("license"), "URL")
    licenses_json          = _json(m.get("license"))
    links_json             = _json(m.get("link"))
    primary_url            = m.get("resource", {}).get("primary", {}).get("URL")

    issn_json              = _json(m.get("ISSN"))
    issn_type_json         = _json(m.get("issn-type"))
    isbn_type_json         = _json(m.get("isbn-type"))
    alternative_id_json    = _json(m.get("alternative-id"))

    subjects               = "; ".join(m.get("subject") or []) if m.get("subject") else None
    subjects_json          = _json(m.get("subject"))
    language               = m.get("language")

    funders_json           = _json(m.get("funder"))

    reference_count        = m.get("reference-count")
    is_referenced_by_count = m.get("is-referenced-by-count")
    references_json        = _json(m.get("reference"))

    update_to_json         = _json(m.get("update-to"))
    update_policy          = m.get("update-policy")
    update_type            = _first(m.get("update-to"), "type")

    publisher              = m.get("publisher")
    member                 = m.get("member")
    prefix                 = m.get("prefix")
    doi                    = m.get("DOI")
    url                    = m.get("URL")
    type_                  = m.get("type")
    subtype                = m.get("subtype")

    archive_json           = _json(m.get("archive"))
    content_domain_json    = _json(m.get("content-domain"))
    assertion_json         = _json(m.get("assertion"))

    institution_name       = _first(m.get("institution"), "name")
    institution_json       = _json(m.get("institution"))

    group_title            = m.get("group-title")
    source                 = m.get("source")
    score                  = m.get("score")
    abstract_raw           = m.get("abstract")

    return {
        "doi": doi,
        "url": url,
        "primary_url": primary_url,
        "title": title,
        "original_title": original_title,
        "short_title": short_title,
        "subtitle": subtitle,
        "type": type_,
        "subtype": subtype,
        "prefix": prefix,
        "publisher": publisher,
        "container_title": container_title,
        "short_container_title": short_container_title,
        "institution_name": institution_name,
        "created_date": created_date,
        "posted_date": posted_date,
        "deposited_date": deposited_date,
        "indexed_date": indexed_date,
        "issued_date": issued_date,
        "published_online_date": published_online_date,
        "published_print_date": published_print_date,
        "accepted_date": accepted_date,
        "approved_date": approved_date,
        "authors": authors_pretty,
        "authors_json": authors_json,
        "editors_json": editors_json,
        "translators_json": translators_json,
        "chairs_json": chairs_json,
        "contributors_json": contributors_json,
        "license_url": license_url,
        "licenses_json": licenses_json,
        "links_json": links_json,
        "subjects": subjects,
        "subjects_json": subjects_json,
        "language": language,
        "issn_json": issn_json,
        "issn_type_json": issn_type_json,
        "isbn_type_json": isbn_type_json,
        "alternative_id_json": alternative_id_json,
        "funder_json": funders_json,
        "reference_count": reference_count,
        "is_referenced_by_count": is_referenced_by_count,
        "references_json": references_json,
        "is_preprint_of": is_preprint_of,
        "has_preprint": has_preprint,
        "is_version_of": is_version_of,
        "relation_json": relation_json,
        "update_type": update_type,
        "update_policy": update_policy,
        "update_to_json": update_to_json,
        "archive_json": archive_json,
        "content_domain_json": content_domain_json,
        "assertion_json": assertion_json,
        "institution_json": institution_json,
        "group_title": group_title,
        "member": member,
        "source": source,
        "score": score,
        "abstract_raw": abstract_raw,
    }

# ---------------- orchestrator ----------------
def harvest_preprints_dataframe(
    date_start="2015-01-01",
    date_end=None,
    mailto=DEFAULT_MAILTO,
    prefixes=None,
    batch_days=7,
    rows_per_call=1000,
    sort_key="deposited",
    save_parquet_path=None,
    save_csv_path=None,
    save_pkl_path=None,
    save_ndjson_path=None,
    adaptive_threshold=1500,
    min_window_seconds=3600,
    incremental_save_dir=None,
    doi_refetch_enabled=True,
    doi_refetch_threshold=1800,
    doi_refetch_sleep_s=0.05,
):
    if date_end is None:
        date_end = date.today().isoformat()

    start = datetime.fromisoformat(date_start).date()
    end   = datetime.fromisoformat(date_end).date()

    all_rows = []
    wrote_any_json = False

    if save_ndjson_path:
        ndj_is_gz = save_ndjson_path.endswith(".gz")
        ndj_fh = gzip.open(save_ndjson_path, "at", encoding="utf-8") if ndj_is_gz else open(save_ndjson_path, "a", encoding="utf-8")
    else:
        ndj_fh = None

    if incremental_save_dir:
        os.makedirs(incremental_save_dir, exist_ok=True)
        print(f"Incremental saves will be written to: {incremental_save_dir}")

    try:
        window = timedelta(days=batch_days)
        cur = start
        while cur <= end:
            w_end = min(cur + window - timedelta(days=1), end)
            print(f"Window: {cur} → {w_end}")
            batch_rows = []

            start_dt = datetime.combine(cur, datetime.min.time())
            end_dt   = datetime.combine(w_end, datetime.max.time())
            fanout_prefixes = _prefix_fanout(prefixes)

            for from_iso, until_iso, pfx_list in _iter_subwindows_adaptive(
                start_dt, end_dt,
                prefixes=fanout_prefixes,
                sort_key=sort_key,
                threshold=adaptive_threshold,
                min_window_seconds=min_window_seconds
            ):
                def _process_subwindow(from_iso, until_iso, pfx):
                    nonlocal wrote_any_json
                    try:
                        sub_total = _total_results_for_window(from_iso, until_iso, prefix=pfx, sort_key=sort_key)
                    except Exception as e:
                        print(f"Warning: total-results probe failed for {from_iso}–{until_iso} (prefix={pfx}): {e}")
                        sub_total = None

                    use_doi_first = (
                        doi_refetch_enabled and (sub_total is not None) and (sub_total > doi_refetch_threshold)
                    )

                    if use_doi_first:
                        print(f"[DOI-first] {from_iso} → {until_iso} (prefix={pfx}) total={sub_total}")
                        processed = 0
                        doi_gen = _stream_doi_list(from_iso, until_iso, mailto=mailto, prefix=pfx, rows=rows_per_call, sort_key=sort_key)
                        for doi in doi_gen:
                            try:
                                m = _fetch_work_by_doi(doi)
                                if not m:
                                    continue
                                row = _one_row_wide(m)
                                batch_rows.append(row)
                                processed += 1
                                if ndj_fh:
                                    ndj_fh.write(json.dumps(row, ensure_ascii=False) + "\n")
                                    wrote_any_json = True
                                if doi_refetch_sleep_s > 0:
                                    time.sleep(doi_refetch_sleep_s)
                            except Exception as e:
                                print(f"Fetch failed for DOI {doi}: {e}")
                        print(f"Processed {processed} DOIs for window {from_iso} → {until_iso} (prefix={pfx})")
                        return

                    processed = 0
                    if pfx:
                        sub_iter = stream_preprints(
                            from_iso, until_iso,
                            mailto=mailto, prefixes=[pfx],
                            rows=rows_per_call, cursor="*", sort_key=sort_key
                        )
                    else:
                        sub_iter = stream_preprints(
                            from_iso, until_iso,
                            mailto=mailto, prefixes=None,
                            rows=rows_per_call, cursor="*", sort_key=sort_key
                        )
                    for m in sub_iter:
                        row = _one_row_wide(m)
                        batch_rows.append(row)
                        processed += 1
                        if ndj_fh:
                            ndj_fh.write(json.dumps(row, ensure_ascii=False) + "\n")
                            wrote_any_json = True
                    print(f"Processed {processed} items (fast-path) for window {from_iso} → {until_iso} (prefix={pfx})")

                if pfx_list:
                    for p in pfx_list:
                        _process_subwindow(from_iso, until_iso, p)
                else:
                    _process_subwindow(from_iso, until_iso, None)

            if incremental_save_dir and batch_rows:
                batch_df = pd.DataFrame(batch_rows)
                batch_filename = os.path.join(
                    incremental_save_dir,
                    f"crossref_preprints_{cur.strftime('%Y%m%d')}_{w_end.strftime('%Y%m%d')}.parquet"
                )
                batch_df.to_parquet(batch_filename, index=False)
                print(f"Saved batch to: {batch_filename}")

            if batch_rows:
                all_rows.extend(batch_rows)

            cur = w_end + timedelta(days=1)

        df = pd.DataFrame(all_rows)

        if not df.empty:
            df.sort_values(
                ["posted_date","deposited_date","indexed_date","created_date"],
                inplace=True, na_position="last"
            )
            df.drop_duplicates(subset=["doi"], keep="first", inplace=True)

        return df
    finally:
        if ndj_fh:
            ndj_fh.close()
            if wrote_any_json:
                print(f"NDJSON written to: {save_ndjson_path}")

# ---------------- CLI ----------------
def _main():
    p = argparse.ArgumentParser(description="Harvest Crossref posted-content (preprints & related).")
    p.add_argument("--date-start", required=True, help="YYYY-MM-DD")
    p.add_argument("--date-end", default=None, help="YYYY-MM-DD (default: today)")
    p.add_argument("--mailto", default=DEFAULT_MAILTO, help="Contact email for Crossref polite pool")
    p.add_argument("--prefixes", nargs="*", default=None, help="Optional DOI prefixes to restrict (e.g., 10.1101 10.48550)")
    p.add_argument("--batch-days", type=int, default=7)
    p.add_argument("--rows-per-call", type=int, default=1000)
    p.add_argument("--sort-key", default="deposited", choices=["deposited","created","indexed"])
    p.add_argument("--adaptive-threshold", type=int, default=1500)
    p.add_argument("--min-window-seconds", type=int, default=3600)
    p.add_argument("--doi-refetch-enabled", action="store_true", default=True)
    p.add_argument("--doi-refetch-threshold", type=int, default=1800)
    p.add_argument("--doi-refetch-sleep-s", type=float, default=0.05)
    p.add_argument("--output-dir", default="./data/output", help="Folder for final outputs (created if missing)")
    p.add_argument("--save-csv", action="store_true", help="Write final CSV")
    p.add_argument("--save-parquet", action="store_true", help="Write final Parquet")
    p.add_argument("--save-pkl", action="store_true", help="Write final Pickle")
    p.add_argument("--save-ndjson", action="store_true", help="Also write an NDJSON stream while harvesting")
    args = p.parse_args()

    # Resolve output paths
    os.makedirs(args.output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    parquet_path = os.path.join(args.output_dir, f"crossref_preprints_wide_{timestamp}.parquet") if args.save_parquet else None
    csv_path     = os.path.join(args.output_dir, f"crossref_preprints_wide_{timestamp}.csv") if args.save_csv else None
    pkl_path     = os.path.join(args.output_dir, f"crossref_preprints_wide_{timestamp}.pkl") if args.save_pkl else None
    ndjson_path  = os.path.join(args.output_dir, f"crossref_preprints_wide_{timestamp}.ndjson.gz") if args.save_ndjson else None

    df = harvest_preprints_dataframe(
        date_start=args.date_start,
        date_end=args.date_end,
        mailto=args.mailto,
        prefixes=args.prefixes,
        batch_days=args.batch_days,
        rows_per_call=args.rows_per_call,
        sort_key=args.sort_key,
        save_parquet_path=parquet_path,
        save_csv_path=csv_path,
        save_pkl_path=pkl_path,
        save_ndjson_path=ndjson_path,
        adaptive_threshold=args.adaptive_threshold,
        min_window_seconds=args.min_window_seconds,
        incremental_save_dir=None,
        doi_refetch_enabled=args.doi_refetch_enabled,
        doi_refetch_threshold=args.doi_refetch_threshold,
        doi_refetch_sleep_s=args.doi_refetch_sleep_s,
    )

    # Persist finals
    if df is None or df.empty:
        print("No records harvested for the chosen window.")
        return 0

    if csv_path:
        df.to_csv(csv_path, index=False)
        print(f"Saved CSV → {csv_path}")
    if parquet_path:
        try:
            df.to_parquet(parquet_path, index=False)
            print(f"Saved Parquet → {parquet_path}")
        except Exception as e:
            print(f"[warn] Could not write Parquet: {e}")
    if pkl_path:
        df.to_pickle(pkl_path)
        print(f"Saved Pickle → {pkl_path}")

    print("Final shape:", df.shape)
    return 0

if __name__ == "__main__":
    sys.exit(_main())
