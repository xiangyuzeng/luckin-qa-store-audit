"""
Layer 4: Trend Data — produces 06_trend_data.csv.
ALL inspections across ALL dates for trend analysis.
"""

import logging
from pathlib import Path

import pandas as pd

from config import SEVERITY_ORDER

logger = logging.getLogger(__name__)


def build_trend_data(full_df, output_dir):
    """Build trend data and write 06_trend_data.csv.

    Args:
        full_df: Full DataFrame with ALL months, with module assignments
        output_dir: Path to output directory

    Returns dict with trend summary for 00_summary.json.
    """
    output_dir = Path(output_dir)

    if full_df.empty:
        pd.DataFrame().to_csv(output_dir / '06_trend_data.csv', index=False, encoding='utf-8-sig')
        return _empty_result()

    df = full_df.copy()

    # Add month column
    if '_parsed_date' in df.columns:
        df['month'] = df['_parsed_date'].dt.strftime('%Y-%m')
    elif 'check_date' in df.columns:
        df['month'] = pd.to_datetime(df['check_date'], errors='coerce').dt.strftime('%Y-%m')
    else:
        df['month'] = ''

    # Mark anomalous scores
    df['anomaly'] = df['report_score'] < 0

    # Per-row trend data — includes O items (raw data export)
    output_cols = [
        'check_date', 'month', 'store_serial', 'store_name', 'checklist_number',
        'check_category', 'report_score',
        'module_id', 'module_cn', 'severity', 'deduction_value',
        'label', 'matched_clause', 'match_method',
        'check_items', 'issue_description',
        'anomaly',
    ]
    available_cols = [c for c in output_cols if c in df.columns]
    trend_df = df[available_cols].copy()

    # Clean issue_description
    if 'issue_description' in trend_df.columns:
        trend_df['issue_description'] = trend_df['issue_description'].fillna('').astype(str)
        trend_df.loc[trend_df['issue_description'] == 'nan', 'issue_description'] = ''

    trend_df = trend_df.sort_values(['check_date', 'store_serial', 'module_id'])
    trend_df.to_csv(output_dir / '06_trend_data.csv', index=False, encoding='utf-8-sig')
    logger.info(f"Wrote 06_trend_data.csv ({len(trend_df)} rows)")

    # Summary stats
    months = sorted(df['month'].dropna().unique().tolist())

    # Per-month averages (from unique inspections, excluding anomalous)
    inspections = df[~df['anomaly']].drop_duplicates('checklist_number')
    avg_by_month = {}
    insp_by_month = {}
    for m in months:
        m_insp = inspections[inspections['month'] == m]
        insp_by_month[m] = len(m_insp)
        if not m_insp.empty:
            avg_by_month[m] = round(m_insp['report_score'].astype(float).mean(), 1)

    return {
        'months_covered': months,
        'inspections_per_month': insp_by_month,
        'average_score_per_month': avg_by_month,
    }


def _empty_result():
    return {
        'months_covered': [],
        'inspections_per_month': {},
        'average_score_per_month': {},
    }
