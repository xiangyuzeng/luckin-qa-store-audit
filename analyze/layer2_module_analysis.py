"""
Layer 2: Module Analysis — produces 02_module_analysis.csv + 03_store_module_matrix.csv.
"""

import logging
from pathlib import Path

import pandas as pd

from config import MODULES, MODULE_ORDER

logger = logging.getLogger(__name__)


def build_module_analysis(df, total_stores, output_dir):
    """Build module analysis and write 02 + 03 CSV files.

    Args:
        df: Current month DataFrame with module assignments
        total_stores: Total unique stores
        output_dir: Path to output directory

    Returns dict with summary metrics for 00_summary.json.
    """
    output_dir = Path(output_dir)

    if df.empty:
        _write_empty(output_dir)
        return _empty_result()

    # Issues: exclude O items (deduction_value == 0) for counting
    issues = df[df['deduction_value'] != 0].copy()

    # --- FILE 02: Module Analysis ---
    modules = []
    for mid in MODULE_ORDER:
        mod_info = MODULES[mid]
        mod_issues = issues[issues['module_id'] == mid]

        sev_counts = mod_issues['severity'].value_counts().to_dict() if not mod_issues.empty else {}
        affected = mod_issues['store_serial'].nunique() if not mod_issues.empty else 0
        affected_pct = round(affected / max(total_stores, 1) * 100, 1)
        total_deduction = float(mod_issues['deduction_value'].sum()) if not mod_issues.empty else 0

        # Top 3 raw Label values
        top_labels = ''
        if not mod_issues.empty and 'label' in mod_issues.columns:
            top = mod_issues['label'].value_counts().head(3).index.tolist()
            top_labels = ', '.join(str(l) for l in top)

        modules.append({
            'module_id': mid,
            'module_name_cn': mod_info['cn'],
            'module_name_en': mod_info['en'],
            'issue_count': len(mod_issues),
            'total_deduction': round(total_deduction, 1),
            's_count': sev_counts.get('S', 0),
            'm_count': sev_counts.get('M', 0),
            'g_count': sev_counts.get('G', 0),
            'l_count': sev_counts.get('L', 0),
            'affected_store_count': affected,
            'affected_store_pct': affected_pct,
            'is_systemic': affected_pct > 50,
            'has_critical': sev_counts.get('S', 0) > 0,
            'top_labels': top_labels,
        })

    # Also handle unmapped (module_id == 0)
    unmapped_issues = issues[issues['module_id'] == 0]
    if not unmapped_issues.empty:
        sev_counts = unmapped_issues['severity'].value_counts().to_dict()
        modules.append({
            'module_id': 0,
            'module_name_cn': '未分类',
            'module_name_en': 'Uncategorized',
            'issue_count': len(unmapped_issues),
            'total_deduction': round(float(unmapped_issues['deduction_value'].sum()), 1),
            's_count': sev_counts.get('S', 0),
            'm_count': sev_counts.get('M', 0),
            'g_count': sev_counts.get('G', 0),
            'l_count': sev_counts.get('L', 0),
            'affected_store_count': unmapped_issues['store_serial'].nunique(),
            'affected_store_pct': round(unmapped_issues['store_serial'].nunique() / max(total_stores, 1) * 100, 1),
            'is_systemic': False,
            'has_critical': sev_counts.get('S', 0) > 0,
            'top_labels': ', '.join(unmapped_issues['label'].value_counts().head(3).index.tolist()) if 'label' in unmapped_issues.columns else '',
        })

    mod_df = pd.DataFrame(modules)
    # Sort by total_deduction ASC (most negative first), then issue_count DESC
    mod_df = mod_df.sort_values(
        ['total_deduction', 'issue_count'], ascending=[True, False]
    )
    mod_df.to_csv(output_dir / '02_module_analysis.csv', index=False, encoding='utf-8-sig')
    logger.info(f"Wrote 02_module_analysis.csv ({len(mod_df)} modules)")

    # --- FILE 03: Store × Module Matrix ---
    if not issues.empty:
        # Filter to modules 1-12 for matrix columns
        matrix_issues = issues[issues['module_id'].between(1, 12)]
        cross = matrix_issues.pivot_table(
            index=['store_serial', 'store_name'],
            columns='module_id',
            values='deduction_value',
            aggfunc='sum',
            fill_value=0,
        )
        # Ensure all 12 module columns exist
        for mid in MODULE_ORDER:
            if mid not in cross.columns:
                cross[mid] = 0
        cross = cross[MODULE_ORDER]
        # Rename columns to module_1, module_2, etc.
        cross.columns = [f"module_{mid}" for mid in MODULE_ORDER]
        cross['total_deduction'] = cross.sum(axis=1)

        # Add report_score from the data
        store_scores = df.drop_duplicates('store_serial').set_index('store_serial')['report_score']
        cross = cross.reset_index()
        cross['report_score'] = cross['store_serial'].map(store_scores)

        cross = cross.sort_values('total_deduction')
    else:
        cross = pd.DataFrame(columns=['store_serial', 'store_name'] +
                             [f"module_{mid}" for mid in MODULE_ORDER] +
                             ['total_deduction', 'report_score'])

    cross.to_csv(output_dir / '03_store_module_matrix.csv', index=False, encoding='utf-8-sig')
    logger.info(f"Wrote 03_store_module_matrix.csv ({len(cross)} stores)")

    # Summary for JSON
    ranked = mod_df[mod_df['module_id'] > 0].head(3)
    systemic = mod_df[(mod_df['is_systemic']) & (mod_df['module_id'] > 0)]['module_name_cn'].tolist()
    critical = mod_df[(mod_df['has_critical']) & (mod_df['module_id'] > 0)]['module_name_cn'].tolist()

    return {
        'module_df': mod_df,
        'total_issues': int(len(issues)),
        'top_3_modules': ranked[['module_id', 'module_name_cn', 'issue_count', 'total_deduction']].to_dict('records'),
        'systemic_modules': systemic,
        'critical_modules': critical,
    }


def _write_empty(output_dir):
    output_dir = Path(output_dir)
    pd.DataFrame(columns=[
        'module_id', 'module_name_cn', 'module_name_en', 'issue_count',
        'total_deduction', 's_count', 'm_count', 'g_count', 'l_count',
        'affected_store_count', 'affected_store_pct', 'is_systemic', 'has_critical', 'top_labels',
    ]).to_csv(output_dir / '02_module_analysis.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame().to_csv(output_dir / '03_store_module_matrix.csv', index=False, encoding='utf-8-sig')


def _empty_result():
    return {'module_df': pd.DataFrame(), 'total_issues': 0,
            'top_3_modules': [], 'systemic_modules': [], 'critical_modules': []}
