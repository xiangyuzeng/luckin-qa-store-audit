#!/usr/bin/env python3
"""
QA Store Audit — Multi-Category Analysis (Part A + Part B).
Part A: Standard monthly analysis (Files 01-07 + 00_summary.json)
Part B: 3-Category comparison (Files 10-16)

Usage:
    python3 category_analysis.py --input data.xlsx [--checklist checklist.xlsx] [--primary-month 2026-01]
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from clause_matcher import assign_modules, build_audit_trail, cross_validate_severity, load_checklist
from config import (
    ATTRIBUTION_KEYWORDS, DEDUCTION_SEVERITY, MODULES, MODULE_ORDER,
    REPORT_AUTHOR, SLA_STANDARDS,
)
from utils import (
    detect_anomalous_scores, filter_by_month, get_analysis_month,
    normalize_columns, parse_date_column,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger(__name__)

# Categories to analyze (exclude legacy)
VALID_CATEGORIES = [
    'Store food safety self-check',
    'Store food safety audit',
    'Area food safety Check',
]
CATEGORY_SHORT = {
    'Store food safety self-check': 'self_check',
    'Store food safety audit': 'qa_audit',
    'Area food safety Check': 'area_check',
}
# Priority for "latest" selection (higher = preferred when same date)
CATEGORY_PRIORITY = {
    'Store food safety audit': 3,
    'Area food safety Check': 2,
    'Store food safety self-check': 1,
}


def read_excel(path):
    logger.info(f"Reading: {path}")
    df = pd.read_excel(path, engine='openpyxl')
    logger.info(f"Raw: {len(df)} rows, {len(df.columns)} columns")
    df = normalize_columns(df)
    if 'check_date' in df.columns:
        df['check_date'] = parse_date_column(df['check_date'])
    if 'deduction_value' in df.columns:
        df['deduction_value'] = pd.to_numeric(df['deduction_value'], errors='coerce').fillna(0)
    if 'report_score' in df.columns:
        df['report_score'] = pd.to_numeric(df['report_score'], errors='coerce')
    return df


def exclude_legacy(df):
    """Remove US Store Food Safety Audit rows."""
    if 'check_category' in df.columns:
        mask = df['check_category'] != 'US Store Food Safety Audit'
        removed = len(df) - mask.sum()
        if removed > 0:
            logger.info(f"Excluded {removed} legacy 'US Store Food Safety Audit' rows")
        return df[mask].copy()
    return df


def get_latest_per_store(df):
    """Get latest inspection per store. Prefer QA audit > area > self-check on same date."""
    if df.empty:
        return df
    df = df.copy()
    df['_cat_priority'] = df['check_category'].map(CATEGORY_PRIORITY).fillna(0)
    # Get unique inspections
    inspections = df.drop_duplicates('checklist_number').copy()
    inspections = inspections.sort_values(
        ['check_date', '_cat_priority'], ascending=[False, False]
    )
    latest = inspections.groupby('store_serial').first().reset_index()
    # Get all rows for those checklists
    latest_checklists = set(latest['checklist_number'])
    result = df[df['checklist_number'].isin(latest_checklists)].copy()
    result = result.drop(columns=['_cat_priority'], errors='ignore')
    return result


def estimate_responsibility(desc):
    if not isinstance(desc, str) or not desc.strip() or desc == 'nan':
        return '未知'
    dl = desc.lower()
    for kw in ATTRIBUTION_KEYWORDS.get('机修', []):
        if kw.lower() in dl: return '机修'
    for kw in ATTRIBUTION_KEYWORDS.get('营建', []):
        if kw.lower() in dl: return '营建'
    for kw in ATTRIBUTION_KEYWORDS.get('门店', []):
        if kw.lower() in dl: return '门店'
    return '门店'


# ============================================================
# PART A: Standard Monthly Analysis
# ============================================================

def part_a_store_performance(latest_df, full_df, year, month, output_dir):
    """File 01: store_performance.csv"""
    clean, anomalous = detect_anomalous_scores(
        latest_df.drop_duplicates('checklist_number')
    )

    # Prior month
    prior_month = month - 1 if month > 1 else 12
    prior_year = year if month > 1 else year - 1
    prior_df = filter_by_month(full_df, prior_year, prior_month)
    prior_lookup = {}
    if not prior_df.empty:
        prior_latest = get_latest_per_store(prior_df)
        prior_insp = prior_latest.drop_duplicates('checklist_number')
        prior_clean, _ = detect_anomalous_scores(prior_insp)
        for _, r in prior_clean.iterrows():
            prior_lookup[r['store_serial']] = float(r['report_score'])

    rows = []
    for _, sr in clean.iterrows():
        serial = sr['store_serial']
        cl = sr['checklist_number']
        insp_rows = latest_df[latest_df['checklist_number'] == cl]
        real = insp_rows[insp_rows['deduction_value'] != 0]
        sev = real['severity'].value_counts().to_dict() if not real.empty else {}
        prior_score = prior_lookup.get(serial)
        rows.append({
            'store_name': sr.get('store_name', ''),
            'store_serial_number': serial,
            'department_code': sr.get('dept_code', ''),
            'city': sr.get('city', ''),
            'area': sr.get('area', ''),
            'check_date': str(sr.get('check_date', ''))[:10],
            'report_score': float(sr['report_score']),
            'check_category': sr.get('check_category', ''),
            'store_level': sr.get('store_level', ''),
            'total_issues': len(real),
            'total_deduction': float(real['deduction_value'].sum()) if not real.empty else 0,
            's_count': sev.get('S', 0), 'm_count': sev.get('M', 0),
            'g_count': sev.get('G', 0), 'l_count': sev.get('L', 0),
            'o_count': int((insp_rows['deduction_value'] == 0).sum()),
            'prior_month_score': prior_score,
            'score_change': round(float(sr['report_score']) - prior_score, 1) if prior_score else None,
            'anomaly': False,
        })
    for _, sr in anomalous.iterrows():
        rows.append({
            'store_name': sr.get('store_name', ''), 'store_serial_number': sr.get('store_serial', ''),
            'department_code': sr.get('dept_code', ''), 'city': sr.get('city', ''),
            'area': sr.get('area', ''), 'check_date': str(sr.get('check_date', ''))[:10],
            'report_score': float(sr['report_score']), 'check_category': sr.get('check_category', ''),
            'store_level': sr.get('store_level', ''), 'total_issues': 0, 'total_deduction': 0,
            's_count': 0, 'm_count': 0, 'g_count': 0, 'l_count': 0, 'o_count': 0,
            'prior_month_score': None, 'score_change': None, 'anomaly': True,
        })

    result = pd.DataFrame(rows).sort_values('report_score', ascending=False)
    result.to_csv(output_dir / '01_store_performance.csv', index=False, encoding='utf-8-sig')
    logger.info(f"Wrote 01_store_performance.csv ({len(result)} stores)")
    return result


def part_a_module_analysis(latest_df, total_stores, output_dir):
    """Files 02 + 03."""
    issues = latest_df[latest_df['deduction_value'] != 0].copy()
    modules = []
    for mid in MODULE_ORDER:
        mi = MODULES[mid]
        mod_iss = issues[issues['module_id'] == mid]
        sev = mod_iss['severity'].value_counts().to_dict() if not mod_iss.empty else {}
        affected = mod_iss['store_serial'].nunique() if not mod_iss.empty else 0
        top_labels = ', '.join(mod_iss['label'].value_counts().head(3).index.tolist()) if not mod_iss.empty and 'label' in mod_iss.columns else ''
        modules.append({
            'module_id': mid, 'module_name_cn': mi['cn'], 'module_name_en': mi['en'],
            'issue_count': len(mod_iss),
            'total_deduction': round(float(mod_iss['deduction_value'].sum()), 1) if not mod_iss.empty else 0,
            's_count': sev.get('S', 0), 'm_count': sev.get('M', 0),
            'g_count': sev.get('G', 0), 'l_count': sev.get('L', 0),
            'affected_store_count': affected,
            'affected_store_pct': round(affected / max(total_stores, 1) * 100, 1),
            'is_systemic': affected > total_stores * 0.5,
            'has_critical': sev.get('S', 0) > 0,
            'top_labels': top_labels,
        })
    mod_df = pd.DataFrame(modules).sort_values(['total_deduction', 'issue_count'], ascending=[True, False])
    mod_df.to_csv(output_dir / '02_module_analysis.csv', index=False, encoding='utf-8-sig')

    # Cross-tab
    if not issues.empty:
        matrix = issues[issues['module_id'].between(1, 12)].pivot_table(
            index=['store_serial', 'store_name'], columns='module_id',
            values='deduction_value', aggfunc='sum', fill_value=0
        )
        for mid in MODULE_ORDER:
            if mid not in matrix.columns: matrix[mid] = 0
        matrix = matrix[MODULE_ORDER]
        matrix.columns = [f'module_{m}' for m in MODULE_ORDER]
        matrix['total_deduction'] = matrix.sum(axis=1)
        scores = latest_df.drop_duplicates('store_serial').set_index('store_serial')['report_score']
        matrix = matrix.reset_index()
        matrix['report_score'] = matrix['store_serial'].map(scores)
        matrix = matrix.sort_values('total_deduction')
    else:
        matrix = pd.DataFrame()
    matrix.to_csv(output_dir / '03_store_module_matrix.csv', index=False, encoding='utf-8-sig')
    logger.info("Wrote 02 + 03")
    return mod_df


def part_a_risk_detail(latest_df, output_dir):
    """Files 04 + 05."""
    issues = latest_df[latest_df['deduction_value'] != 0].copy()
    if not issues.empty:
        issues['issue_description'] = issues['issue_description'].fillna('').astype(str)
        issues.loc[issues['issue_description'] == 'nan', 'issue_description'] = ''
        issues['estimated_responsibility'] = issues['issue_description'].apply(estimate_responsibility)

    cols = ['store_name', 'store_serial', 'check_date', 'checklist_number',
            'module_id', 'module_cn', 'matched_clause', 'label', 'check_items',
            'issue_description', 'deduction_type', 'severity', 'deduction_value',
            'checker', 'match_method', 'estimated_responsibility', 'check_category']
    avail = [c for c in cols if c in issues.columns]
    risk_df = issues[avail].sort_values(['severity', 'store_name']) if not issues.empty else pd.DataFrame(columns=cols)
    risk_df.to_csv(output_dir / '04_risk_detail.csv', index=False, encoding='utf-8-sig')

    s_items = risk_df[risk_df['severity'] == 'S'] if 'severity' in risk_df.columns else pd.DataFrame()
    s_items.to_csv(output_dir / '05_s_items_detail.csv', index=False, encoding='utf-8-sig')
    logger.info(f"Wrote 04 ({len(risk_df)} issues) + 05 ({len(s_items)} S-items)")
    return risk_df, s_items


def part_a_trend(full_df, output_dir):
    """File 06."""
    df = full_df.copy()
    df['month'] = df['check_date'].dt.strftime('%Y-%m') if hasattr(df['check_date'], 'dt') else ''
    df['anomaly'] = df['report_score'] < 0
    df['issue_description'] = df['issue_description'].fillna('').astype(str)
    df.loc[df['issue_description'] == 'nan', 'issue_description'] = ''
    cols = ['check_date', 'month', 'store_serial', 'store_name', 'checklist_number',
            'check_category', 'report_score', 'module_id', 'module_cn', 'severity',
            'deduction_value', 'label', 'matched_clause', 'check_items',
            'issue_description', 'anomaly']
    avail = [c for c in cols if c in df.columns]
    df[avail].sort_values(['check_date', 'store_serial']).to_csv(
        output_dir / '06_trend_data.csv', index=False, encoding='utf-8-sig'
    )
    logger.info(f"Wrote 06_trend_data.csv ({len(df)} rows)")


# ============================================================
# PART B: 3-Category Comparison
# ============================================================

def part_b_coverage(full_df, output_dir):
    """File 10: category_coverage_by_month.csv"""
    df = full_df.copy()
    df['month'] = df['check_date'].dt.strftime('%Y-%m')
    rows = []
    for (month, cat), g in df.groupby(['month', 'check_category']):
        inspections = g.drop_duplicates('checklist_number')
        rows.append({
            'month': month, 'category': cat,
            'stores_inspected': g['store_serial'].nunique(),
            'store_names': ', '.join(sorted(g['store_name'].unique())),
            'total_inspections': len(inspections),
            'total_rows': len(g),
            'checkers': ', '.join(sorted(g['checker'].unique())),
            'checker_positions': ', '.join(sorted(g['checker_position'].unique())) if 'checker_position' in g.columns else '',
        })
    cov_df = pd.DataFrame(rows).sort_values(['month', 'category'])
    cov_df.to_csv(output_dir / '10_category_coverage_by_month.csv', index=False, encoding='utf-8-sig')
    logger.info(f"Wrote 10_category_coverage_by_month.csv")
    return cov_df


def part_b_score_comparison(month_df, output_dir):
    """File 11: jan_score_comparison.csv"""
    # Get latest score per (store, category)
    df = month_df.drop_duplicates('checklist_number').copy()
    df = df.sort_values('check_date', ascending=False).groupby(
        ['store_serial', 'store_name', 'check_category']
    ).first().reset_index()

    stores = df['store_serial'].unique()
    rows = []
    for serial in sorted(stores):
        sdf = df[df['store_serial'] == serial]
        store_name = sdf.iloc[0]['store_name']
        cats = set(sdf['check_category'])
        if len(cats) < 2:
            continue  # Only show stores with 2+ categories

        row = {'store_name': store_name, 'store_serial': serial}
        for cat, short in CATEGORY_SHORT.items():
            cr = sdf[sdf['check_category'] == cat]
            if not cr.empty:
                r = cr.iloc[0]
                row[f'{short}_score'] = float(r['report_score'])
                row[f'{short}_date'] = str(r['check_date'])[:10]
                row[f'{short}_checker'] = r.get('checker', '')
            else:
                row[f'{short}_score'] = None
                row[f'{short}_date'] = ''
                row[f'{short}_checker'] = ''

        sc = row.get('self_check_score')
        qa = row.get('qa_audit_score')
        ar = row.get('area_check_score')
        row['gap_self_vs_qa'] = round(sc - qa, 1) if sc is not None and qa is not None else None
        row['gap_self_vs_area'] = round(sc - ar, 1) if sc is not None and ar is not None else None
        row['gap_qa_vs_area'] = round(qa - ar, 1) if qa is not None and ar is not None else None
        rows.append(row)

    result = pd.DataFrame(rows)
    result.to_csv(output_dir / '11_jan_score_comparison.csv', index=False, encoding='utf-8-sig')
    logger.info(f"Wrote 11_jan_score_comparison.csv ({len(result)} stores)")
    return result


def part_b_severity_comparison(month_df, output_dir):
    """File 12: jan_category_severity.csv"""
    issues = month_df[month_df['deduction_value'] != 0].copy()
    rows = []
    for cat in VALID_CATEGORIES:
        cat_all = month_df[month_df['check_category'] == cat]
        cat_issues = issues[issues['check_category'] == cat]
        inspections = cat_all.drop_duplicates('checklist_number')
        n_insp = len(inspections)
        if n_insp == 0:
            continue
        scores = inspections['report_score'].astype(float)
        sev = cat_issues['severity'].value_counts().to_dict() if not cat_issues.empty else {}
        total_iss = len(cat_issues)

        # Top 3 modules
        if not cat_issues.empty:
            top_mods = cat_issues.groupby('module_cn').size().sort_values(ascending=False).head(3)
            top_str = ', '.join(f"{m}({c})" for m, c in top_mods.items())
        else:
            top_str = ''

        rows.append({
            'category': cat,
            'inspections_count': n_insp,
            'stores_inspected': cat_all['store_serial'].nunique(),
            'avg_score': round(scores.mean(), 1),
            'total_issues': total_iss,
            'avg_issues_per_inspection': round(total_iss / n_insp, 1),
            'avg_deduction_per_inspection': round(cat_issues['deduction_value'].sum() / n_insp, 1) if not cat_issues.empty else 0,
            's_count': sev.get('S', 0), 'm_count': sev.get('M', 0),
            'g_count': sev.get('G', 0), 'l_count': sev.get('L', 0),
            'o_count': int((cat_all['deduction_value'] == 0).sum()),
            's_pct': round(sev.get('S', 0) / max(total_iss, 1) * 100, 1),
            'm_pct': round(sev.get('M', 0) / max(total_iss, 1) * 100, 1),
            'g_pct': round(sev.get('G', 0) / max(total_iss, 1) * 100, 1),
            'l_pct': round(sev.get('L', 0) / max(total_iss, 1) * 100, 1),
            'top_3_modules': top_str,
        })
    result = pd.DataFrame(rows)
    result.to_csv(output_dir / '12_jan_category_severity.csv', index=False, encoding='utf-8-sig')
    logger.info(f"Wrote 12_jan_category_severity.csv")
    return result


def part_b_module_by_category(month_df, output_dir):
    """File 13: jan_module_by_category.csv"""
    issues = month_df[month_df['deduction_value'] != 0]
    rows = []
    for mid in MODULE_ORDER:
        mi = MODULES[mid]
        row = {'module_id': mid, 'module_cn': mi['cn']}
        for cat, short in CATEGORY_SHORT.items():
            ci = issues[(issues['check_category'] == cat) & (issues['module_id'] == mid)]
            row[f'{short}_issues'] = len(ci)
            row[f'{short}_deduction'] = round(float(ci['deduction_value'].sum()), 1) if not ci.empty else 0
        mod_all = issues[issues['module_id'] == mid]
        row['total_issues'] = len(mod_all)
        row['total_deduction'] = round(float(mod_all['deduction_value'].sum()), 1) if not mod_all.empty else 0
        rows.append(row)
    result = pd.DataFrame(rows)
    result.to_csv(output_dir / '13_jan_module_by_category.csv', index=False, encoding='utf-8-sig')
    logger.info(f"Wrote 13_jan_module_by_category.csv")
    return result


def part_b_cross_category_gaps(month_df, output_dir):
    """File 14: jan_cross_category_gaps.csv"""
    issues = month_df[month_df['deduction_value'] != 0]
    # Find stores with 2+ categories
    store_cats = month_df.groupby('store_serial')['check_category'].apply(set).to_dict()
    rows = []
    for serial, cats in store_cats.items():
        if len(cats) < 2:
            continue
        store_name = month_df[month_df['store_serial'] == serial]['store_name'].iloc[0]
        store_issues = issues[issues['store_serial'] == serial]

        mods_by_cat = {}
        for cat, short in CATEGORY_SHORT.items():
            ci = store_issues[store_issues['check_category'] == cat]
            mods_by_cat[short] = set(ci['module_cn'].unique()) if not ci.empty else set()

        sc = mods_by_cat.get('self_check', set())
        qa = mods_by_cat.get('qa_audit', set())
        ar = mods_by_cat.get('area_check', set())

        rows.append({
            'store_name': store_name,
            'categories_count': len(cats),
            'self_check_modules': ', '.join(sorted(sc)) if sc else '',
            'qa_audit_modules': ', '.join(sorted(qa)) if qa else '',
            'area_check_modules': ', '.join(sorted(ar)) if ar else '',
            'only_in_qa_audit': ', '.join(sorted(qa - sc)) if qa - sc else '',
            'only_in_area_check': ', '.join(sorted(ar - sc)) if ar - sc else '',
        })
    result = pd.DataFrame(rows)
    result.to_csv(output_dir / '14_jan_cross_category_gaps.csv', index=False, encoding='utf-8-sig')
    logger.info(f"Wrote 14_jan_cross_category_gaps.csv ({len(result)} stores)")
    return result


def part_b_inspector(full_df, output_dir):
    """File 15: inspector_analysis.csv"""
    issues = full_df[full_df['deduction_value'] != 0]
    full_df['month'] = full_df['check_date'].dt.strftime('%Y-%m')
    rows = []
    group_cols = ['checker', 'check_category']
    if 'checker_position' in full_df.columns:
        group_cols = ['checker', 'checker_position', 'check_category']

    for grp, g in full_df.groupby(group_cols, dropna=False):
        if len(group_cols) == 3:
            checker, pos, cat = grp
        else:
            checker, cat = grp
            pos = ''
        inspections = g.drop_duplicates('checklist_number')
        n_insp = len(inspections)
        gi = issues[(issues['checker'] == checker) & (issues['check_category'] == cat)]
        sev = gi['severity'].value_counts().to_dict() if not gi.empty else {}
        months_active = sorted(g['month'].unique())
        rows.append({
            'checker_name': checker,
            'checker_position': pos,
            'category': cat,
            'total_inspections': n_insp,
            'stores_inspected': g['store_serial'].nunique(),
            'avg_score_given': round(inspections['report_score'].astype(float).mean(), 1),
            'avg_issues_per_inspection': round(len(gi) / max(n_insp, 1), 1),
            'avg_deduction_per_inspection': round(gi['deduction_value'].sum() / max(n_insp, 1), 1) if not gi.empty else 0,
            's_found': sev.get('S', 0), 'm_found': sev.get('M', 0),
            'g_found': sev.get('G', 0), 'l_found': sev.get('L', 0),
            'months_active': ', '.join(months_active),
            'last_inspection_date': str(g['check_date'].max())[:10],
        })
    result = pd.DataFrame(rows).sort_values('avg_issues_per_inspection', ascending=False)
    result.to_csv(output_dir / '15_inspector_analysis.csv', index=False, encoding='utf-8-sig')
    logger.info(f"Wrote 15_inspector_analysis.csv ({len(result)} inspectors)")
    return result


def part_b_completeness(full_df, output_dir):
    """File: 05_inspection_completeness.csv"""
    full_df['month'] = full_df['check_date'].dt.strftime('%Y-%m')
    all_stores = sorted(full_df['store_serial'].unique())
    all_months = sorted(full_df['month'].unique())
    store_names = full_df.drop_duplicates('store_serial').set_index('store_serial')['store_name'].to_dict()

    rows = []
    for month in all_months:
        mdf = full_df[full_df['month'] == month]
        for serial in all_stores:
            sdf = mdf[mdf['store_serial'] == serial]
            cats = set(sdf['check_category'].unique()) if not sdf.empty else set()
            has_sc = 'Store food safety self-check' in cats
            has_qa = 'Store food safety audit' in cats
            has_ac = 'Area food safety Check' in cats
            count = sum([has_sc, has_qa, has_ac])
            gaps = []
            if not has_sc: gaps.append('self-check')
            if not has_qa: gaps.append('QA audit')
            if not has_ac: gaps.append('area check')
            rows.append({
                'store_name': store_names.get(serial, serial),
                'month': month,
                'has_self_check': has_sc,
                'has_qa_audit': has_qa,
                'has_area_check': has_ac,
                'categories_count': count,
                'gap_note': f"Missing {', '.join(gaps)}" if gaps else 'Complete',
            })
    result = pd.DataFrame(rows)
    result.to_csv(output_dir / '16_inspection_completeness.csv', index=False, encoding='utf-8-sig')
    logger.info(f"Wrote 16_inspection_completeness.csv")
    return result


def build_category_summary(cov_df, score_comp, sev_comp, module_comp, gaps_df, inspector_df,
                           primary_month, full_df):
    """File 16: category_summary.json"""
    today = datetime.now()
    py, pm = int(primary_month[:4]), int(primary_month[5:7])

    # Category stats
    cat_stats = {}
    for _, r in sev_comp.iterrows():
        short = CATEGORY_SHORT.get(r['category'], r['category'])
        cat_stats[short] = {
            'inspections': int(r['inspections_count']),
            'stores': int(r['stores_inspected']),
            'avg_score': float(r['avg_score']),
            'avg_issues': float(r['avg_issues_per_inspection']),
            'checkers': list(full_df[
                (full_df['check_category'] == r['category']) &
                (full_df['check_date'].dt.strftime('%Y-%m') == primary_month)
            ]['checker'].unique()),
        }

    # Score comparison
    sc_avg = sev_comp[sev_comp['category'] == 'Store food safety self-check']['avg_score'].values
    qa_avg = sev_comp[sev_comp['category'] == 'Store food safety audit']['avg_score'].values
    ar_avg = sev_comp[sev_comp['category'] == 'Area food safety Check']['avg_score'].values
    sc_val = float(sc_avg[0]) if len(sc_avg) else None
    qa_val = float(qa_avg[0]) if len(qa_avg) else None
    ar_val = float(ar_avg[0]) if len(ar_avg) else None
    gap = round(sc_val - qa_val, 1) if sc_val and qa_val else None

    interp = ''
    if gap is not None:
        if gap > 3:
            interp = f"Self-check scores are on average {gap} points higher than QA audit — suggests leniency in self-assessment"
        elif gap > 0:
            interp = f"Self-check scores are {gap} points higher than QA audit — minor gap"
        else:
            interp = f"QA audit scores are {abs(gap)} points higher than self-check"

    # Blind spots
    modules_only_qa = []
    modules_only_area = []
    if not module_comp.empty:
        for _, r in module_comp.iterrows():
            if r.get('qa_audit_issues', 0) > 0 and r.get('self_check_issues', 0) == 0:
                modules_only_qa.append({'module': r['module_cn'], 'issue_count': int(r['qa_audit_issues'])})
            if r.get('area_check_issues', 0) > 0 and r.get('self_check_issues', 0) == 0:
                modules_only_area.append({'module': r['module_cn'], 'issue_count': int(r['area_check_issues'])})

    # Coverage history
    full_df['month'] = full_df['check_date'].dt.strftime('%Y-%m')
    coverage_hist = {}
    for cat, short in CATEGORY_SHORT.items():
        cat_months = sorted(full_df[full_df['check_category'] == cat]['month'].unique())
        last_m = cat_months[-1] if cat_months else None
        gap_months = 0
        if last_m:
            ly, lm = int(last_m[:4]), int(last_m[5:7])
            gap_months = (today.year - ly) * 12 + today.month - lm
        coverage_hist[short] = {
            'last_month': last_m,
            'months_active': cat_months,
            'total_inspections': int(full_df[full_df['check_category'] == cat].drop_duplicates('checklist_number').shape[0]),
            'gap_months': gap_months,
        }

    # Key findings (Chinese)
    findings = []
    if gap and gap > 3:
        findings.append(f"门店自检评分偏高（平均{sc_val}分 vs QA审计{qa_val}分，差距{gap}分），存在自检宽松倾向，建议加强自检标准培训。")
    elif gap and gap > 0:
        findings.append(f"门店自检评分略高于QA审计（{sc_val}分 vs {qa_val}分，差距{gap}分），差异在合理范围内。")

    ac_hist = coverage_hist.get('area_check', {})
    if ac_hist.get('gap_months', 0) > 1:
        findings.append(f"区经检查已中断{ac_hist['gap_months']}个月，最后一次为{ac_hist.get('last_month', 'N/A')}。建议尽快恢复区经巡检覆盖。")

    if modules_only_qa:
        mod_names = '、'.join(m['module'] for m in modules_only_qa)
        findings.append(f"QA审计发现了自检未覆盖的模块问题：{mod_names}。这些模块是门店自检的盲区，需要重点加强。")

    if modules_only_area:
        mod_names = '、'.join(m['module'] for m in modules_only_area)
        findings.append(f"区经检查发现了自检未覆盖的模块：{mod_names}。")

    # Stores never QA audited or area checked
    all_stores = set(full_df['store_name'].unique())
    qa_stores = set(full_df[full_df['check_category'] == 'Store food safety audit']['store_name'].unique())
    area_stores = set(full_df[full_df['check_category'] == 'Area food safety Check']['store_name'].unique())
    never_qa = sorted(all_stores - qa_stores)
    never_area = sorted(all_stores - area_stores)
    if never_qa:
        findings.append(f"以下门店从未接受QA审计：{'、'.join(never_qa)}。建议优先安排审计。")
    if never_area:
        findings.append(f"以下门店从未接受区经检查：{'、'.join(never_area)}。")

    # Recommendations
    recs = [
        "建议每月至少完成1次门店自检、1次QA审计（覆盖全部门店）、1次区经检查（覆盖50%以上门店）。",
        "针对自检评分偏高的情况，建议引入自检结果与QA审计结果的交叉验证机制。",
    ]
    if ac_hist.get('gap_months', 0) > 1:
        recs.append("立即恢复区经巡检，优先覆盖近3个月未被区经检查的门店。")
    if never_qa:
        recs.append(f"优先对{'、'.join(never_qa[:3])}等门店安排QA审计。")

    return {
        'analysis_month': primary_month,
        'analysis_date': today.strftime('%Y-%m-%d'),
        'categories_present': [CATEGORY_SHORT[c] for c in VALID_CATEGORIES
                               if c in full_df[full_df['check_date'].dt.strftime('%Y-%m') == primary_month]['check_category'].values],
        'category_stats': cat_stats,
        'score_comparison': {
            'avg_self_check_score': sc_val,
            'avg_qa_audit_score': qa_val,
            'avg_area_check_score': ar_val,
            'self_check_vs_qa_gap': gap,
            'interpretation': interp,
        },
        'blind_spots': {
            'modules_only_qa_catches': modules_only_qa,
            'modules_only_area_catches': modules_only_area,
        },
        'coverage_history': coverage_hist,
        'coverage_gaps': {
            'stores_never_qa_audited': never_qa,
            'stores_never_area_checked': never_area,
        },
        'key_findings_cn': findings,
        'recommendations_cn': recs,
    }


# ============================================================
# STDOUT Summary
# ============================================================

def print_summary(store_perf, mod_df, s_items, sev_comp, score_comp, inspector_df,
                  module_comp, gaps_df, cat_summary, primary_month):
    yr, mo = primary_month[:4], primary_month[5:7]

    print(f"\n{'='*70}")
    print(f"  QA STORE AUDIT — {yr}年{mo}月 (Multi-Category Analysis)")
    print(f"{'='*70}")

    # Part A
    print(f"\n--- PART A: Monthly Analysis ({primary_month}) ---\n")
    valid = store_perf[store_perf['anomaly'] == False] if 'anomaly' in store_perf.columns else store_perf
    if not valid.empty:
        print(f"  Average score: {valid['report_score'].mean():.1f}")
        print(f"  Store scores (ranked):")
        for _, r in valid.iterrows():
            ch = f" ({r['score_change']:+.0f})" if pd.notna(r.get('score_change')) else ''
            print(f"    {r['store_name']:20s} {r['report_score']:5.0f}{ch}  [{r.get('check_category','')}]")

    print(f"\n  Top 3 risk modules:")
    for _, r in mod_df.head(3).iterrows():
        print(f"    Module {r['module_id']:2d} {r['module_name_cn']:12s} — {r['issue_count']} issues, {r['total_deduction']} deductions")

    print(f"\n  S-items: {len(s_items)}")
    for _, r in s_items.iterrows():
        desc = str(r.get('issue_description', ''))[:70]
        print(f"    - {r.get('store_name','')} | {r.get('module_cn','')} | {desc}")

    # Part B
    print(f"\n--- PART B: 3-Category Comparison ---\n")

    # Category severity
    if not sev_comp.empty:
        print("  Category comparison:")
        print(f"    {'Category':<35s} {'Insp':>5s} {'AvgScore':>9s} {'AvgIss':>7s} {'S':>3s} {'M':>3s} {'G':>3s} {'L':>3s}")
        for _, r in sev_comp.iterrows():
            print(f"    {r['category']:<35s} {r['inspections_count']:>5d} {r['avg_score']:>9.1f} "
                  f"{r['avg_issues_per_inspection']:>7.1f} {r['s_count']:>3d} {r['m_count']:>3d} "
                  f"{r['g_count']:>3d} {r['l_count']:>3d}")

    # Score comparison
    if score_comp is not None and not score_comp.empty:
        print(f"\n  Score comparison (stores with 2+ categories):")
        for _, r in score_comp.iterrows():
            parts = [r['store_name']]
            if pd.notna(r.get('self_check_score')): parts.append(f"Self:{r['self_check_score']:.0f}")
            if pd.notna(r.get('qa_audit_score')): parts.append(f"QA:{r['qa_audit_score']:.0f}")
            if pd.notna(r.get('area_check_score')): parts.append(f"Area:{r['area_check_score']:.0f}")
            if pd.notna(r.get('gap_self_vs_qa')): parts.append(f"Gap(S-Q):{r['gap_self_vs_qa']:+.0f}")
            print(f"    {' | '.join(parts)}")

    # Inspector ranking
    if not inspector_df.empty:
        print(f"\n  Inspector thoroughness ranking:")
        for _, r in inspector_df.head(6).iterrows():
            print(f"    {r['checker_name']:20s} [{r['category'][:15]}] "
                  f"avg_issues={r['avg_issues_per_inspection']:.1f} "
                  f"avg_score={r['avg_score_given']:.1f} "
                  f"S={r['s_found']} M={r['m_found']}")

    # Blind spots
    bs = cat_summary.get('blind_spots', {})
    qa_only = bs.get('modules_only_qa_catches', [])
    if qa_only:
        print(f"\n  Blind spots (QA catches but self-check misses):")
        for m in qa_only:
            print(f"    - {m['module']} ({m['issue_count']} issues)")

    # Key findings in Chinese
    print(f"\n--- 关键发现 (Key Findings) ---\n")
    for i, f in enumerate(cat_summary.get('key_findings_cn', []), 1):
        print(f"  {i}. {f}")

    print(f"\n--- 建议 (Recommendations) ---\n")
    for i, r in enumerate(cat_summary.get('recommendations_cn', []), 1):
        print(f"  {i}. {r}")
    print()


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description='QA Multi-Category Analysis')
    parser.add_argument('--input', '-i', required=True, help='Path to inspection Excel')
    parser.add_argument('--checklist', '-c', help='Path to QA checklist Excel (optional)')
    parser.add_argument('--primary-month', '-m', help='Primary analysis month YYYY-MM (auto-detect if omitted)')
    parser.add_argument('--output-dir', '-o', default=None, help='Output directory')
    args = parser.parse_args()

    if not os.path.exists(args.input):
        logger.error(f"File not found: {args.input}")
        sys.exit(1)

    script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    output_base = Path(args.output_dir) if args.output_dir else script_dir.parent / 'output'

    # 1. Read data
    df = read_excel(args.input)
    df = exclude_legacy(df)

    # 2. Load checklist + assign modules
    checklist_df = None
    if args.checklist and os.path.exists(args.checklist):
        checklist_df = load_checklist(args.checklist)
    df = assign_modules(df, checklist_df)
    df = cross_validate_severity(df)

    # 3. Determine primary month
    if args.primary_month:
        primary_month = args.primary_month
    else:
        # Auto-detect: latest month with 2+ categories
        df['_month'] = df['check_date'].dt.strftime('%Y-%m')
        month_cats = df.groupby('_month')['check_category'].nunique()
        multi = month_cats[month_cats >= 2]
        if not multi.empty:
            primary_month = multi.index.max()
        else:
            primary_month = df['_month'].max()
        logger.info(f"Auto-detected primary month: {primary_month}")

    py, pm = int(primary_month[:4]), int(primary_month[5:7])

    # Output dirs
    main_out = output_base / f'qa-analysis-{primary_month}'
    cat_out = main_out / 'category-comparison'
    main_out.mkdir(parents=True, exist_ok=True)
    cat_out.mkdir(parents=True, exist_ok=True)

    # 4. Filter primary month
    month_df = filter_by_month(df, py, pm)
    latest_df = get_latest_per_store(month_df)
    total_stores = month_df['store_serial'].nunique()

    print(f"\nPrimary month: {primary_month}, Stores: {total_stores}")
    cats = month_df['check_category'].unique()
    print(f"Categories present: {', '.join(sorted(cats))}")

    # ===== PART A =====
    logger.info("=== PART A: Standard Monthly Analysis ===")
    store_perf = part_a_store_performance(latest_df, df, py, pm, main_out)
    mod_df = part_a_module_analysis(latest_df, total_stores, main_out)
    risk_df, s_items = part_a_risk_detail(latest_df, main_out)
    part_a_trend(df, main_out)

    # Audit trail
    audit_df = build_audit_trail(month_df)
    audit_df.to_csv(main_out / '07_module_mapping_audit.csv', index=False, encoding='utf-8-sig')

    # Summary JSON (Part A)
    valid_perf = store_perf[store_perf['anomaly'] == False]
    summary_a = {
        'schema_version': '3.0',
        'metadata': {
            'analysis_month': primary_month, 'analysis_date': datetime.now().strftime('%Y-%m-%d'),
            'total_stores': total_stores, 'report_id': f'LCNA-QA-{py}-{pm:03d}',
            'author': REPORT_AUTHOR,
        },
        'layer1_store_performance': {
            'avg_score': round(float(valid_perf['report_score'].mean()), 1) if not valid_perf.empty else 0,
            'store_scores': valid_perf[['store_name', 'store_serial_number', 'report_score']].to_dict('records'),
        },
        'layer2_module_analysis': {
            'total_issues': int(mod_df['issue_count'].sum()),
            'top_modules': mod_df.head(3)[['module_id', 'module_name_cn', 'issue_count', 'total_deduction']].to_dict('records'),
        },
        'layer3_risk_analysis': {
            's_items_count': len(s_items),
        },
    }
    with open(main_out / '00_summary.json', 'w', encoding='utf-8') as f:
        json.dump(summary_a, f, ensure_ascii=False, indent=2, default=str)

    # ===== PART B =====
    logger.info("=== PART B: Category Comparison ===")
    cov_df = part_b_coverage(df, cat_out)
    score_comp = part_b_score_comparison(month_df, cat_out)
    sev_comp = part_b_severity_comparison(month_df, cat_out)
    module_comp = part_b_module_by_category(month_df, cat_out)
    gaps_df = part_b_cross_category_gaps(month_df, cat_out)
    inspector_df = part_b_inspector(df, cat_out)
    completeness = part_b_completeness(df, cat_out)

    cat_summary = build_category_summary(
        cov_df, score_comp, sev_comp, module_comp, gaps_df, inspector_df,
        primary_month, df
    )
    with open(cat_out / '17_category_summary.json', 'w', encoding='utf-8') as f:
        json.dump(cat_summary, f, ensure_ascii=False, indent=2, default=str)
    logger.info("Wrote 17_category_summary.json")

    # ===== PRINT =====
    print_summary(store_perf, mod_df, s_items, sev_comp, score_comp,
                  inspector_df, module_comp, gaps_df, cat_summary, primary_month)

    print(f"Output files:")
    for d in [main_out, cat_out]:
        for f in sorted(d.iterdir()):
            if f.is_file():
                print(f"  {f.relative_to(output_base)!s:55s} ({f.stat().st_size:,} bytes)")


if __name__ == '__main__':
    main()
