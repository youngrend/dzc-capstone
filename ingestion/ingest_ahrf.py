"""
AHRF Ingestion Script
=====================
Reads AHRF county-level category CSVs (hp, hf, pop) for each release year,
filters to selected variables, joins on FIPS code, converts to Parquet,
uploads to GCS.

Pipeline: Category CSVs → Filter → Join on FIPS → Parquet → GCS

Usage:
    python ingestion/ingest_ahrf.py

Expects:
    - .env file in project root with GCS_BUCKET_NAME and GCP_PROJECT_ID
    - AHRF category CSVs in their original unzipped directory structure
    - GCS bucket already exists
"""

import logging
import os
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage

# ---------------------------------------------------------------------------
# LOGGING SETUP
# ---------------------------------------------------------------------------
# logging is Python's built-in alternative to print() for diagnostic output.
# Unlike print, every message carries a severity level (INFO / WARNING / ERROR)
# and a timestamp. That makes it easy to grep for problems and redirect output
# to a file without changing the code.
#
# basicConfig sets the output format for the whole process.
# %(asctime)s   — human-readable timestamp
# %(levelname)s — INFO / WARNING / ERROR
# %(message)s   — what you actually logged
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# getLogger(__name__) creates a logger named after this module.
# __name__ is a built-in string Python sets to the current module's name
# (e.g. "ingest_ahrf"). Using it means log lines are traceable to their source
# file, which matters when multiple modules emit logs at the same time.
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. CONFIGURATION
# ---------------------------------------------------------------------------
load_dotenv()

# Type hints (the ": Optional[str]" part) are annotations — they don't change
# how the code runs, but they tell readers (and tools like mypy or your IDE)
# what type each variable is expected to hold.
#
# Optional[str] means "this is either a str or None".
# os.getenv() returns None when the key is missing, so Optional[str] is the
# honest type here. Without the hint you'd have just str, which is a lie.
GCS_BUCKET_NAME: Optional[str] = os.getenv("GCS_BUCKET_NAME")
GCP_PROJECT_ID: Optional[str] = os.getenv("GCP_PROJECT_ID")

PROJECT_ROOT: Path = Path(__file__).parent.parent
PARQUET_OUTPUT_DIR: Path = PROJECT_ROOT / "data" / "parquet"


# ---------------------------------------------------------------------------
# 2. SOURCE CONTRACT
# ---------------------------------------------------------------------------
JOIN_KEY: str = "fips_st_cnty"

# List[str] means "a list whose items are all strings".
# Dict[str, str] means "a dict where both keys and values are strings".
# These come from the typing module imported above.
GEO_COLS: List[str] = [
    "fips_st_cnty",
    "st_name",
    "st_name_abbrev",
    "cnty_name",
    "fips_st",
    "fips_cnty",
]

HP_VARIABLES: Dict[str, str] = {
    "md_nf_prim_care_pc_excl_rsdnt":  "primary_care_mds",
    "do_nf_prim_care_pc_excl_rsdnt":  "primary_care_dos",
    "np_npi":                          "nurse_practitioners",
    "pa_npi":                          "physician_assistants",
    "dent_nf_prvprac":                 "dentists_private_practice",
    "md_nf_psych_all_pc":              "psychiatrists",
}

HF_VARIABLES: Dict[str, str] = {
    "fedly_qualfd_hlth_ctr":           "fqhcs",
    "comn_hlth_ctr_grants_only":       "community_health_centers",
    "stgh":                            "short_term_gen_hospitals",
    "stgh_hosp_beds":                  "hospital_beds",
    "critcl_access_hosp":              "critical_access_hospitals",
    "rural_hlth_clincs":               "rural_health_clinics",
}

POP_VARIABLES: Dict[str, str] = {
    "popn_est":                        "population",
    "popn_est_ge65":                   "population_65plus",
    "pers_lt_fpl_pct":                 "pct_below_poverty",
    "pers_noins_lt65_pct":             "pct_uninsured_under65",
    "medn_hhi_saipe":                  "median_household_income",
    "unemply_rate_ge16":               "unemployment_rate",
}


# ---------------------------------------------------------------------------
# 3. RELEASE FILE MAP
# ---------------------------------------------------------------------------
RELEASES: List[Dict] = [
    {
        "release": "2022-2023",
        "year_suffixes": ["22", "21", "23", "20"],
        "subfiles": {
            "hp":  "AHRF_2022-2023_CSV/AHRF_CSV_2022-2023/DATA/CSV Files by Categories/ahrf2023HP.csv",
            "hf":  "AHRF_2022-2023_CSV/AHRF_CSV_2022-2023/DATA/CSV Files by Categories/ahrf2023HF.csv",
            "pop": "AHRF_2022-2023_CSV/AHRF_CSV_2022-2023/DATA/CSV Files by Categories/ahrf2023POP.csv",
        },
    },
    {
        "release": "2023-2024",
        "year_suffixes": ["23", "22", "24", "21"],
        "subfiles": {
            "hp":  "AHRF 2023-2024 CSV/CSV Files by Categories/ahrf2024hp.csv",
            "hf":  "AHRF 2023-2024 CSV/CSV Files by Categories/ahrf2024hf.csv",
            "pop": "AHRF 2023-2024 CSV/CSV Files by Categories/ahrf2024pop.csv",
        },
    },
    {
        "release": "2024-2025",
        "year_suffixes": ["24", "23", "25", "22"],
        "subfiles": {
            "hp":  "AHRF_2024-2025_CSV/NCHWA-2024-2025+AHRF+COUNTY+CSV/AHRF2025hp.csv",
            "hf":  "AHRF_2024-2025_CSV/NCHWA-2024-2025+AHRF+COUNTY+CSV/AHRF2025hf.csv",
            "pop": "AHRF_2024-2025_CSV/NCHWA-2024-2025+AHRF+COUNTY+CSV/AHRF2025pop.csv",
        },
    },
]

SUBFILE_VARIABLES: Dict[str, Dict[str, str]] = {
    "hp":  HP_VARIABLES,
    "hf":  HF_VARIABLES,
    "pop": POP_VARIABLES,
}


# ---------------------------------------------------------------------------
# 4. COLUMN MATCHING
# ---------------------------------------------------------------------------

def resolve_columns(
    base_names: Dict[str, str],
    csv_columns: List[str],
    year_suffixes: List[str],
) -> Dict[str, str]:
    """
    Match base variable names to actual CSV column names by trying
    year suffixes in order.

    Returns: {actual_csv_column_name: friendly_rename}
    """
    csv_cols_lower: Dict[str, str] = {c.lower().strip(): c for c in csv_columns}
    matched: Dict[str, str] = {}

    for base, friendly_name in base_names.items():
        # next() with a generator expression replaces the old for-loop + bool flag.
        #
        # A generator expression (the part inside the outer parentheses) is a
        # lazy sequence — Python evaluates each item only when next() asks for
        # the next one. That means it stops as soon as it finds a match instead
        # of building the whole list first.
        #
        # next(iterator, default) returns the first item, or `default` (None
        # here) if the iterator is exhausted without finding anything.
        # This avoids a StopIteration exception that bare next() would raise.
        match = next(
            (
                csv_cols_lower[f"{base}_{s}".lower()]
                for s in year_suffixes
                if f"{base}_{s}".lower() in csv_cols_lower
            ),
            None,
        )
        if match:
            matched[match] = friendly_name
        else:
            logger.warning(f"No match for '{base}' with suffixes {year_suffixes}")

    return matched


# ---------------------------------------------------------------------------
# 5. READ ONE CATEGORY CSV
# ---------------------------------------------------------------------------
# passthrough_cols are included verbatim (no suffix matching).
# Used to pull GEO columns from the HP file in a single read pass
# instead of opening that file again later.

def read_category_csv(
    csv_path: Path,
    variables: Dict[str, str],
    year_suffixes: List[str],
    passthrough_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
    logger.info(f"Reading: {csv_path.name}")

    if not csv_path.exists():
        logger.error(f"File not found: {csv_path} — skipping")
        return pd.DataFrame()

    # try / except — the try block runs normally; if any line in it raises
    # an exception (an error object), Python immediately jumps to the except
    # block. Without this, a corrupt CSV or encoding problem would crash the
    # entire pipeline mid-run. With it, we log the error and return an empty
    # DataFrame so process_release can skip this file gracefully.
    #
    # "except Exception as e" catches virtually any error and binds it to the
    # name `e` so we can include the error message in the log.
    try:
        # nrows=0 reads only the header row — fast way to get column names
        # without pulling the full 22 MB file into memory.
        header_df = pd.read_csv(csv_path, nrows=0, encoding="latin-1")
        all_columns = header_df.columns.tolist()
        logger.info(f"Total columns in file: {len(all_columns)}")

        col_mapping = resolve_columns(variables, all_columns, year_suffixes)
        logger.info(f"Matched: {len(col_mapping)} of {len(variables)}")

        csv_cols_lower: Dict[str, str] = {c.lower().strip(): c for c in all_columns}

        fips_actual = csv_cols_lower.get(JOIN_KEY.lower())
        if not fips_actual:
            logger.error(f"Join key '{JOIN_KEY}' not found — skipping")
            return pd.DataFrame()

        # Resolve passthrough columns (e.g. GEO_COLS) case-insensitively.
        # We skip JOIN_KEY itself — it's already captured above as fips_actual.
        passthrough_actual: Dict[str, str] = {}
        if passthrough_cols:
            for col in passthrough_cols:
                if col.lower() == JOIN_KEY.lower():
                    continue
                if col.lower() in csv_cols_lower:
                    passthrough_actual[csv_cols_lower[col.lower()]] = col

        # Read only the columns we need in a single pass — not the full
        # 1,800-column file. Combining variable and passthrough columns here
        # eliminates the need to re-open the HP file just for GEO data.
        use_cols = (
            [fips_actual]
            + list(col_mapping.keys())
            + list(passthrough_actual.keys())
        )
        df = pd.read_csv(csv_path, usecols=use_cols, encoding="latin-1")

        rename_map = {fips_actual: JOIN_KEY}
        rename_map.update(col_mapping)
        rename_map.update(passthrough_actual)
        df = df.rename(columns=rename_map)

        df[JOIN_KEY] = df[JOIN_KEY].astype(str).str.strip()

        # Coerce analytical variable columns to numeric.
        # AHRF uses blanks, dots, and whitespace for missing values.
        # errors="coerce" turns anything unparseable into NaN.
        for col in col_mapping.values():
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.strip(), errors="coerce"
                )

        # Strip whitespace from passthrough (string) columns.
        for col in passthrough_actual.values():
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        logger.info(f"Rows: {len(df)}, Output columns: {list(df.columns)}")
        return df

    except Exception as e:
        logger.error(f"Error reading {csv_path}: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# 6. PROCESS ONE RELEASE
# ---------------------------------------------------------------------------

def process_release(release_info: Dict) -> None:
    release: str = release_info["release"]
    year_suffixes: List[str] = release_info["year_suffixes"]

    logger.info(f"Processing release: {release}")

    # --- Step A: read each category CSV ---
    # For the HP file we pass GEO_COLS as passthrough_cols so GEO data arrives
    # in the same read — no second file open needed (was 3 reads, now 2).
    category_dfs: Dict[str, pd.DataFrame] = {}
    for cat_key, csv_rel_path in release_info["subfiles"].items():
        csv_path = PROJECT_ROOT / csv_rel_path
        variables = SUBFILE_VARIABLES[cat_key]
        extra = GEO_COLS if cat_key == "hp" else None
        df = read_category_csv(csv_path, variables, year_suffixes, passthrough_cols=extra)
        if not df.empty:
            category_dfs[cat_key] = df

    if not category_dfs:
        logger.error("No category files loaded — skipping release")
        return

    # --- Step B: extract GEO columns from the already-loaded HP DataFrame ---
    # The original code re-opened the HP file here. We don't need to because
    # passthrough_cols above already loaded the GEO columns into category_dfs["hp"].
    geo_df: Optional[pd.DataFrame] = None
    if "hp" in category_dfs:
        hp_df = category_dfs["hp"]
        geo_present = [c for c in GEO_COLS if c in hp_df.columns and c != JOIN_KEY]
        if geo_present:
            geo_df = hp_df[[JOIN_KEY] + geo_present].copy()
            # Drop GEO columns from the HP analytical DataFrame so they don't
            # appear twice after the merge in Step D.
            category_dfs["hp"] = hp_df.drop(columns=geo_present)
            logger.info(f"GEO columns extracted from HP: {list(geo_df.columns)}")

    # --- Step C: join category DataFrames on FIPS ---
    cat_keys = list(category_dfs.keys())
    merged = category_dfs[cat_keys[0]]
    for cat_key in cat_keys[1:]:
        merged = merged.merge(category_dfs[cat_key], on=JOIN_KEY, how="outer")
    logger.info(f"Merged shape: {merged.shape}")

    # --- Step D: attach GEO columns ---
    if geo_df is not None:
        merged = merged.merge(geo_df, on=JOIN_KEY, how="left")

    # --- Step E: add release year for downstream partitioning ---
    merged["release_year"] = release

    # --- Step F: reorder — GEO first, then variables, then release_year ---
    geo_first = [c for c in GEO_COLS if c in merged.columns]
    var_cols = [c for c in merged.columns if c not in GEO_COLS and c != "release_year"]
    col_order = geo_first + sorted(var_cols) + ["release_year"]
    merged = merged[col_order]

    logger.info(f"Final shape: {merged.shape}")
    logger.info(f"Columns: {list(merged.columns)}")

    # --- Step G: write Parquet ---
    try:
        PARQUET_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        parquet_filename = f"ahrf_{release.replace('-', '_')}.parquet"
        parquet_path = PARQUET_OUTPUT_DIR / parquet_filename
        merged.to_parquet(parquet_path, index=False, engine="pyarrow")
        size_kb = parquet_path.stat().st_size / 1024
        logger.info(f"Parquet written: {parquet_path} ({size_kb:.0f} KB)")
    except Exception as e:
        logger.error(f"Error writing Parquet: {e}")
        return

    # --- Step H: upload to GCS ---
    upload_to_gcs(parquet_path, f"raw/{parquet_filename}")
    logger.info(f"Done: {release}")


# ---------------------------------------------------------------------------
# 7. GCS UPLOAD
# ---------------------------------------------------------------------------
def upload_to_gcs(local_path: Path, gcs_blob_name: str) -> None:
    if not GCS_BUCKET_NAME:
        logger.warning("GCS_BUCKET_NAME not set — skipping upload")
        return

    try:
        logger.info(f"Uploading to gs://{GCS_BUCKET_NAME}/{gcs_blob_name}...")
        client = storage.Client(project=GCP_PROJECT_ID)
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(gcs_blob_name)
        blob.upload_from_filename(str(local_path))
        logger.info("Upload complete.")
    except Exception as e:
        logger.error(f"Error uploading to GCS: {e}")


# ---------------------------------------------------------------------------
# 8. MAIN
# ---------------------------------------------------------------------------
def main() -> None:
    # Warn early about missing env vars so the cause is obvious at startup.
    # We warn rather than exit because the pipeline can still run locally
    # without GCS — the upload step is skipped gracefully when the bucket
    # name is absent.
    if not GCS_BUCKET_NAME:
        logger.warning("GCS_BUCKET_NAME not set in .env — GCS uploads will be skipped")
    if not GCP_PROJECT_ID:
        logger.warning("GCP_PROJECT_ID not set in .env — GCS client may fail")

    logger.info("Starting AHRF Ingestion Pipeline")
    logger.info(f"Project:  {GCP_PROJECT_ID}")
    logger.info(f"Bucket:   {GCS_BUCKET_NAME}")
    logger.info(f"Root:     {PROJECT_ROOT}")
    logger.info(f"Releases: {len(RELEASES)}")

    for release_info in RELEASES:
        process_release(release_info)

    logger.info("All releases processed.")


if __name__ == "__main__":
    main()
