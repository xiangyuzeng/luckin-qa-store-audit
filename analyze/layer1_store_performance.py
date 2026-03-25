"""
Layer 1: Store Performance — produces 01_store_performance.csv.
"""

import logging
from pathlib import Path

import pandas as pd

from utils import detect_anomalous_scores, filter_by_month, get_latest_inspection_per_store

logger = logging.getLogger(__name__)


def build_store_performance(current_month_df, full_df, analysis_month, prior_month, output_dir):
    """Build store performance and write 01_store_performance.csv.

    Args:
        current_month_df: DataFrame filtered to analysis month (with module assignments)
        full_df: Full DataFrame (all months, with module assignments)
        analysis_month: (year, month) tuple
        prior_month: (year, month) tuple
        output_dir: Path to output directory

    Returns dict with summary stats and store_df for other layers.
    """
    year, month = analysis_month
    prior_year, prior_month_num = prior_month
    output_dir = Path(output_dir)

    if current_month_df.empty:
        logger.warning(f"No data for {year}-{month:02d}")
        _write_empty(output_dir)
        return _empty_result()

    # Get latest inspection per store
    latest = get_latest_inspection_per_store(current_month_df)
    clean, anomalous = detect_anomalous_scores(latest)

    # Get all rows for the latest inspections (for severity counting)
    latest_checklists = set(clean['checklist_number'].unique())
    latest_rows = current_month_df[
        current_month_df['checklist_number'].isin(latest_checklists)
    ]

    # Per-store severity counts (excluding O items for counts)
    store_stats = []
    for _, store_row in clean.iterrows():
        serial = store_row['store_serial']
        cl_num = store_row['checklist_number']
        store_issues = current_month_df[current_month_df['checklist_number'] == cl_num]

        # Count issues excluding O items (deduction_value == 0)
        real_issues = store_issues[store_issues['deduction_value'] != 0]

        sev_counts = real_issues['severity'].value_counts().to_dict() if not real_issues.empty else {}

        store_stats.append({
            'store_name': str(store_row.get('store_name', '')),
            'store_serial_number': str(serial),
            'department_code': str(store_row.get('dept_code', '')),
            'city': str(store_row.get('city', '')),
            'area': str(store_row.get('area', '')),
            'check_date': str(store_row.get('check_date', ''))[:10],
            'report_score': float(store_row.get('report_score', 0)),
            'check_category': str(store_row.get('check_category', '')),
            'store_level': str(store_row.get('store_level', '')),
            'total_issues': len(real_issues),
            'total_deduction': float(real_issues['deduction_value'].sum()) if not real_issues.empty else 0,
            's_count': sev_counts.get('S', 0),
            'm_count': sev_counts.get('M', 0),
            'g_count': sev_counts.get('G', 0),
            'l_count': sev_counts.get('L', 0),
            'o_count': int((store_issues['deduction_value'] == 0).sum()),
            'prior_month_score': None,
            'score_change': None,
            'anomaly': False,
        })

    # Add anomalous stores
    for _, store_row in anomalous.iterrows():
        store_stats.append({
            'store_name': str(store_row.get('store_name', '')),
            'store_serial_number': str(store_row.get('store_serial', '')),
            'department_code': str(store_row.get('dept_code', '')),
            'city': str(store_row.get('city', '')),
            'area': str(store_row.get('area', '')),
            'check_date': str(store_row.get('check_date', ''))[:10],
            'report_score': float(store_row.get('report_score', 0)),
            'check_category': str(store_row.get('check_category', '')),
            'store_level': str(store_row.get('store_level', '')),
            'total_issues': 0, 'total_deduction': 0,
            's_count': 0, 'm_count': 0, 'g_count': 0, 'l_count': 0, 'o_count': 0,
            'prior_month_score': None, 'score_change': None,
            'anomaly': True,
        })

    # Prior month comparison
    prior_df = filter_by_month(full_df, prior_year, prior_month_num)
    prior_avg = None
    if not prior_df.empty:
        prior_latest = get_latest_inspection_per_store(prior_df)
        prior_clean, _ = detect_anomalous_scores(prior_latest)
        if not prior_clean.empty:
            prior_lookup = {
                str(r['store_serial']): float(r['report_score'])
                for _, r in prior_clean.iterrows()
            }
            prior_avg = round(prior_clean['report_score'].astype(float).mean(), 1)
            for ss in store_stats:
                if not ss['anomaly'] and ss['store_serial_number'] in prior_lookup:
                    ss['prior_month_score'] = prior_lookup[ss['store_serial_number']]
                    ss['score_change'] = round(
                        ss['report_score'] - prior_lookup[ss['store_serial_number']], 1
                    )

    # Build DataFrame and sort by score desc
    result_df = pd.DataFrame(store_stats)
    result_df = result_df.sort_values('report_score', ascending=False)

    # Write CSV
    csv_path = output_dir / '01_store_performance.csv'
    result_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    logger.info(f"Wrote {csv_path} ({len(result_df)} stores)")

    # Compute summary stats (excluding anomalous)
    valid = result_df[~result_df['anomaly']]
    scores = valid['report_score']
    monthly_avg = round(scores.mean(), 1) if not scores.empty else 0

    highest = valid.iloc[0] if not valid.empty else None
    lowest = valid.iloc[-1] if not valid.empty else None

    # Most improved / declined
    with_change = valid[valid['score_change'].notna()]
    most_improved = None
    most_declined = None
    if not with_change.empty:
        best_idx = with_change['score_change'].idxmax()
        worst_idx = with_change['score_change'].idxmin()
        if with_change.loc[best_idx, 'score_change'] > 0:
            r = with_change.loc[best_idx]
            most_improved = {
                'name': r['store_name'], 'serial': r['store_serial_number'],
                'change': float(r['score_change']),
                'current_score': float(r['report_score']),
                'prior_score': float(r['prior_month_score']),
            }
        if with_change.loc[worst_idx, 'score_change'] < 0:
            r = with_change.loc[worst_idx]
            most_declined = {
                'name': r['store_name'], 'serial': r['store_serial_number'],
                'change': float(r['score_change']),
                'current_score': float(r['report_score']),
                'prior_score': float(r['prior_month_score']),
            }

    return {
        'store_df': result_df,
        'latest_rows': latest_rows,
        'monthly_average': monthly_avg,
        'prior_month_average': prior_avg,
        'average_change': round(monthly_avg - prior_avg, 1) if prior_avg is not None else None,
        'total_stores': len(valid),
        'highest_store': {
            'name': highest['store_name'], 'score': float(highest['report_score'])
        } if highest is not None else None,
        'lowest_store': {
            'name': lowest['store_name'], 'score': float(lowest['report_score'])
        } if lowest is not None else None,
        'most_improved': most_improved,
        'most_declined': most_declined,
        'anomalous_count': len(anomalous),
        'store_scores': [
            {'store_name': r['store_name'], 'serial': r['store_serial_number'],
             'score': float(r['report_score']),
             'prior_score': float(r['prior_month_score']) if pd.notna(r.get('prior_month_score')) else None,
             'change': float(r['score_change']) if pd.notna(r.get('score_change')) else None}
            for _, r in valid.iterrows()
        ],
    }


def _write_empty(output_dir):
    pd.DataFrame(columns=[
        'store_name', 'store_serial_number', 'department_code', 'city', 'area',
        'check_date', 'report_score', 'check_category', 'store_level',
        'total_issues', 'total_deduction', 's_count', 'm_count', 'g_count', 'l_count', 'o_count',
        'prior_month_score', 'score_change', 'anomaly',
    ]).to_csv(output_dir / '01_store_performance.csv', index=False, encoding='utf-8-sig')


def _empty_result():
    return {
        'store_df': pd.DataFrame(), 'latest_rows': pd.DataFrame(),
        'monthly_average': 0, 'prior_month_average': None, 'average_change': None,
        'total_stores': 0, 'highest_store': None, 'lowest_store': None,
        'most_improved': None, 'most_declined': None,
        'anomalous_count': 0, 'store_scores': [],
    }
