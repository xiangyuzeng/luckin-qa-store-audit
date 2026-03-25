"""
Layer 3: Risk Detail — produces 04_risk_detail.csv + 05_s_items_detail.csv.
"""

import logging
import re
from pathlib import Path

import pandas as pd

from config import ATTRIBUTION_KEYWORDS, SEVERITY_ORDER

logger = logging.getLogger(__name__)


def build_risk_detail(df, output_dir):
    """Build risk detail and write 04 + 05 CSV files.

    Args:
        df: Current month DataFrame with module assignments
        output_dir: Path to output directory

    Returns dict with severity distribution for 00_summary.json.
    """
    output_dir = Path(output_dir)

    if df.empty:
        _write_empty(output_dir)
        return _empty_result()

    # Exclude O items (deduction_value == 0) for risk detail
    issues = df[df['deduction_value'] != 0].copy()

    if issues.empty:
        _write_empty(output_dir)
        return _empty_result()

    # Assign responsibility based on keywords in issue_description
    issues['estimated_responsibility'] = issues['issue_description'].apply(_estimate_responsibility)

    # Build output columns
    output_cols = [
        'store_name', 'store_serial', 'check_date', 'checklist_number',
        'module_id', 'module_cn', 'matched_clause', 'label',
        'check_items', 'issue_description', 'deduction_type', 'severity',
        'deduction_value', 'checker', 'match_method', 'estimated_responsibility',
    ]
    # Use only columns that exist
    available_cols = [c for c in output_cols if c in issues.columns]

    risk_df = issues[available_cols].copy()

    # Ensure issue_description is exact (empty string for NaN, never generated)
    if 'issue_description' in risk_df.columns:
        risk_df['issue_description'] = risk_df['issue_description'].fillna('').astype(str)
        # Replace 'nan' string with empty
        risk_df.loc[risk_df['issue_description'] == 'nan', 'issue_description'] = ''

    # Sort by severity order then store
    sev_order_map = {s: i for i, s in enumerate(SEVERITY_ORDER)}
    if 'severity' in risk_df.columns:
        risk_df['_sev_order'] = risk_df['severity'].map(sev_order_map).fillna(99)
        risk_df = risk_df.sort_values(['_sev_order', 'store_name', 'module_id'])
        risk_df = risk_df.drop(columns=['_sev_order'])

    # --- FILE 04: Risk Detail (all issues, excluding O) ---
    risk_df.to_csv(output_dir / '04_risk_detail.csv', index=False, encoding='utf-8-sig')
    logger.info(f"Wrote 04_risk_detail.csv ({len(risk_df)} issues)")

    # --- FILE 05: S-items detail ---
    s_items = risk_df[risk_df['severity'] == 'S'] if 'severity' in risk_df.columns else pd.DataFrame()
    s_items.to_csv(output_dir / '05_s_items_detail.csv', index=False, encoding='utf-8-sig')
    logger.info(f"Wrote 05_s_items_detail.csv ({len(s_items)} S-items)")

    # Severity distribution (excluding O items)
    distribution = {}
    percentages = {}
    total = len(issues)
    for sev in SEVERITY_ORDER:
        if sev == 'O':
            continue
        count = int((issues['severity'] == sev).sum())
        distribution[sev] = count
        percentages[sev] = round(count / max(total, 1) * 100, 1)

    # S-items detail for JSON
    s_items_json = []
    for _, row in s_items.iterrows():
        s_items_json.append({
            'store_name': str(row.get('store_name', '')),
            'module_id': int(row.get('module_id', 0)),
            'module_cn': str(row.get('module_cn', '')),
            'matched_clause': str(row.get('matched_clause', '')),
            'check_items': str(row.get('check_items', '')),
            'description': str(row.get('issue_description', '')),
            'deduction': float(row.get('deduction_value', 0)),
            'date': str(row.get('check_date', ''))[:10],
        })

    # Top modules per severity
    m_top = issues[issues['severity'] == 'M'].groupby('module_cn').size().sort_values(ascending=False).head(5).to_dict() if (issues['severity'] == 'M').any() else {}
    g_top = issues[issues['severity'] == 'G'].groupby('module_cn').size().sort_values(ascending=False).head(5).to_dict() if (issues['severity'] == 'G').any() else {}
    l_top = issues[issues['severity'] == 'L'].groupby('module_cn').size().sort_values(ascending=False).head(5).to_dict() if (issues['severity'] == 'L').any() else {}

    # Responsibility summary
    resp_summary = issues['estimated_responsibility'].value_counts().to_dict() if 'estimated_responsibility' in issues.columns else {}

    return {
        'total_issues': total,
        'severity_distribution': distribution,
        'severity_percentages': percentages,
        's_items_count': len(s_items),
        's_items_detail': s_items_json,
        'm_top_modules': m_top,
        'g_top_modules': g_top,
        'l_top_modules': l_top,
        'responsibility_summary': resp_summary,
    }


def _estimate_responsibility(description):
    """Keyword-based responsibility estimation from issue description."""
    if not isinstance(description, str) or not description.strip() or description == 'nan':
        return '未知'

    desc_lower = description.lower()

    # Check 机修 first (equipment/maintenance)
    for kw in ATTRIBUTION_KEYWORDS.get('机修', []):
        if kw.lower() in desc_lower:
            return '机修'

    # Check 营建 (construction/facilities)
    for kw in ATTRIBUTION_KEYWORDS.get('营建', []):
        if kw.lower() in desc_lower:
            return '营建'

    # Check 门店 (store operations)
    for kw in ATTRIBUTION_KEYWORDS.get('门店', []):
        if kw.lower() in desc_lower:
            return '门店'

    return '门店'  # Default to store responsibility if description exists


def _write_empty(output_dir):
    output_dir = Path(output_dir)
    cols = ['store_name', 'store_serial', 'check_date', 'checklist_number',
            'module_id', 'module_cn', 'matched_clause', 'label',
            'check_items', 'issue_description', 'deduction_type', 'severity',
            'deduction_value', 'checker', 'match_method', 'estimated_responsibility']
    pd.DataFrame(columns=cols).to_csv(output_dir / '04_risk_detail.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame(columns=cols).to_csv(output_dir / '05_s_items_detail.csv', index=False, encoding='utf-8-sig')


def _empty_result():
    return {
        'total_issues': 0,
        'severity_distribution': {s: 0 for s in ['S', 'M', 'G', 'L']},
        'severity_percentages': {s: 0.0 for s in ['S', 'M', 'G', 'L']},
        's_items_count': 0, 's_items_detail': [],
        'm_top_modules': {}, 'g_top_modules': {}, 'l_top_modules': {},
        'responsibility_summary': {},
    }
