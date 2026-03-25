"""
Summary JSON builder — produces 00_summary.json.
"""

from datetime import datetime

from config import MODULES, REPORT_AUTHOR, SLA_STANDARDS


def build_summary(l1, l2, l3, l4, analysis_month, prior_month,
                  mapping_stats, data_quality, store_validation):
    """Assemble 00_summary.json from all layer results."""
    year, month = analysis_month
    prior_year, prior_month_num = prior_month

    return {
        'schema_version': '3.0',
        'metadata': {
            'analysis_month': f"{year}-{month:02d}",
            'analysis_month_cn': f"{year}年{month:02d}月",
            'analysis_date': datetime.now().strftime('%Y-%m-%d'),
            'prior_month': f"{prior_year}-{prior_month_num:02d}",
            'has_prior_month': l1.get('prior_month_average') is not None,
            'total_inspections': l1.get('total_stores', 0),
            'total_stores': l1.get('total_stores', 0),
            'report_id': f"LCNA-QA-{year}-{month:03d}",
            'author': REPORT_AUTHOR,
        },

        'store_validation': store_validation,

        'layer1_store_performance': {
            'avg_score': l1.get('monthly_average', 0),
            'prior_month_avg': l1.get('prior_month_average'),
            'avg_change': l1.get('average_change'),
            'max_store': l1.get('highest_store'),
            'min_store': l1.get('lowest_store'),
            'most_improved': l1.get('most_improved'),
            'most_declined': l1.get('most_declined'),
            'store_scores': l1.get('store_scores', []),
        },

        'layer2_module_analysis': {
            'total_issues': l2.get('total_issues', 0),
            'modules': _build_module_list(l2),
            'module_ranking': [m['module_name_cn'] for m in l2.get('top_3_modules', [])],
            'critical_modules': l2.get('critical_modules', []),
            'systemic_modules': l2.get('systemic_modules', []),
            'store_module_connection': _build_store_module_connection(l1, l2),
        },

        'layer3_risk_analysis': {
            'total_issues': l3.get('total_issues', 0),
            'severity_distribution': l3.get('severity_distribution', {}),
            'severity_percentages': l3.get('severity_percentages', {}),
            's_items_count': l3.get('s_items_count', 0),
            's_items_detail': l3.get('s_items_detail', []),
            'm_top_modules': l3.get('m_top_modules', {}),
            'g_top_modules': l3.get('g_top_modules', {}),
            'l_top_modules': l3.get('l_top_modules', {}),
        },

        'layer4_capa': {
            'keyword_attribution': {
                'summary': l3.get('responsibility_summary', {}),
                'disclaimer': '基于关键词的初步归因（仅供参考）— 实际归因需以整改工单系统数据为准',
            },
            'missing_fields': [
                '责任归属（门店/机修/营建）',
                '整改责任人', '整改措施描述',
                '整改完成时间', '验证结果（通过/未通过）',
                '验证人', 'SLA达标状态',
            ],
            'sla_standards': SLA_STANDARDS,
        },

        'trend_summary': {
            'months_covered': l4.get('months_covered', []),
            'average_score_trend': l4.get('average_score_per_month', {}),
            'inspections_per_month': l4.get('inspections_per_month', {}),
        },

        'data_quality': data_quality,
        'mapping_quality': mapping_stats,
    }


def _build_module_list(l2):
    """Build module list for JSON from module_df."""
    mod_df = l2.get('module_df')
    if mod_df is None or mod_df.empty:
        return []

    modules = []
    for _, row in mod_df.iterrows():
        mid = int(row['module_id'])
        if mid == 0:
            continue
        modules.append({
            'module_id': mid,
            'name_cn': row['module_name_cn'],
            'name_en': row['module_name_en'],
            'issue_count': int(row['issue_count']),
            'total_deductions': float(row['total_deduction']),
            's_count': int(row['s_count']),
            'm_count': int(row['m_count']),
            'g_count': int(row['g_count']),
            'l_count': int(row['l_count']),
            'affected_stores': int(row['affected_store_count']),
            'is_systemic': bool(row['is_systemic']),
            'has_critical': bool(row['has_critical']),
        })
    return modules


def _build_store_module_connection(l1, l2):
    """Build store-module cross reference."""
    result = {
        'lowest_store_modules': [],
        'highest_store_zero_deduction_modules': [],
        'most_improved_modules': [],
    }
    # This would require the full issue data — keep as placeholder
    # The detailed cross-reference is in 03_store_module_matrix.csv
    return result
