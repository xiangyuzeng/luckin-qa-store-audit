"""
Shared utilities for QA Store Audit analysis v3.
Column normalization, date parsing, anomaly detection.
"""

import logging

import pandas as pd

from config import MODULES

logger = logging.getLogger(__name__)

# Expected column names (as they appear in the Excel export)
EXPECTED_COLUMNS = {
    'City': 'city',
    'Operational Management Area': 'area',
    'Store serial number': 'store_serial',
    'Department code': 'dept_code',
    'Store name': 'store_name',
    'Checklist number': 'checklist_number',
    'Check category': 'check_category',
    'Check items': 'check_items',
    'Opportunity point description': 'issue_description',
    'Label': 'label',
    'Opportunity point value': 'deduction_value',
    'Deduction type': 'deduction_type',
    'Check route': 'check_route',
    'checker': 'checker',
    'Checker position': 'checker_position',
    'Voiding Approval Position': 'void_approval_position',
    'Voiding Submission Time': 'void_submission_time',
    'Voiding Approval Time': 'void_approval_time',
    'Report score': 'report_score',
    'Store level': 'store_level',
    'Check date': 'check_date',
    'Check report generation time': 'report_gen_time',
}


def normalize_columns(df):
    """Standardize column names: strip whitespace, map to internal names."""
    df.columns = [c.strip() for c in df.columns]
    rename_map = {}
    for orig, internal in EXPECTED_COLUMNS.items():
        if orig in df.columns:
            rename_map[orig] = internal
        else:
            for col in df.columns:
                if col.lower().strip() == orig.lower().strip():
                    rename_map[col] = internal
                    break
    df = df.rename(columns=rename_map)

    mapped = set(rename_map.values())
    expected = set(EXPECTED_COLUMNS.values())
    missing = expected - mapped
    if missing:
        logger.warning(f"Missing expected columns: {missing}")

    return df


def parse_date_column(series):
    """Parse date columns that may be in multiple formats."""
    if series.dtype == 'datetime64[ns]':
        return series

    parsed = pd.to_datetime(series, format='%Y-%m-%d', errors='coerce')
    mask = parsed.isna() & series.notna()
    if mask.any():
        for fmt in ['%m/%d/%Y', '%Y/%m/%d', '%d-%m-%Y', '%Y-%m-%d %H:%M:%S']:
            still_na = parsed.isna() & series.notna()
            if not still_na.any():
                break
            parsed[still_na] = pd.to_datetime(
                series[still_na], format=fmt, errors='coerce'
            )
    return parsed


def parse_datetime_column(series):
    """Parse datetime columns with timestamp."""
    if series.dtype == 'datetime64[ns]':
        return series
    return pd.to_datetime(series, errors='coerce')


def detect_anomalous_scores(df):
    """Identify rows with anomalous negative scores. Returns (clean_df, anomalous_df)."""
    score_col = 'report_score'
    if score_col not in df.columns:
        return df, pd.DataFrame()

    mask = df[score_col] < 0
    anomalous = df[mask].copy()
    clean = df[~mask].copy()

    if len(anomalous) > 0:
        logger.warning(
            f"Found {len(anomalous)} rows with anomalous negative scores "
            f"(min={anomalous[score_col].min()}). Excluding from averages."
        )

    return clean, anomalous


def get_analysis_month(df, specified_month=None):
    """Determine the analysis month. Returns (year, month) and prior month tuples."""
    if 'check_date' not in df.columns:
        raise ValueError("No 'check_date' column found in data")

    dates = parse_date_column(df['check_date'])
    df['_parsed_date'] = dates

    if specified_month:
        parts = specified_month.split('-')
        year, month = int(parts[0]), int(parts[1])
    else:
        valid_dates = dates.dropna()
        if valid_dates.empty:
            raise ValueError("No valid dates found in data")
        latest = valid_dates.max()
        year, month = latest.year, latest.month
        logger.info(f"Auto-detected analysis month: {year}-{month:02d}")

    if month == 1:
        prior_year, prior_month = year - 1, 12
    else:
        prior_year, prior_month = year, month - 1

    return (year, month), (prior_year, prior_month)


def filter_by_month(df, year, month):
    """Filter DataFrame to rows in the specified year-month."""
    if '_parsed_date' not in df.columns:
        df['_parsed_date'] = parse_date_column(df['check_date'])

    mask = (df['_parsed_date'].dt.year == year) & (df['_parsed_date'].dt.month == month)
    return df[mask].copy()


def get_latest_inspection_per_store(df):
    """For each store, keep only the latest inspection (by report_gen_time)."""
    if 'report_gen_time' in df.columns:
        df['_parsed_gen_time'] = parse_datetime_column(df['report_gen_time'])
        sort_col = '_parsed_gen_time'
    elif '_parsed_date' in df.columns:
        sort_col = '_parsed_date'
    else:
        sort_col = 'check_date'

    inspections = df.drop_duplicates(subset=['checklist_number']).copy()
    inspections = inspections.sort_values(sort_col, ascending=False)
    latest = inspections.groupby('store_serial').first().reset_index()
    return latest


def get_module_name(module_id, lang='cn'):
    """Get module name by ID. Returns '未分类'/'Uncategorized' for unknown IDs."""
    mod = MODULES.get(module_id)
    if mod:
        return mod[lang]
    return '未分类' if lang == 'cn' else 'Uncategorized'
