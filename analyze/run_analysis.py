#!/usr/bin/env python3
"""
QA Store Audit v3 — Analysis Pipeline.
Reads inspection Excel + optional QA checklist, outputs 8 structured files.

Usage:
    python3 run_analysis.py --input inspection.xlsx [--checklist checklist.xlsx] [--month 2026-03]
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from clause_matcher import assign_modules, build_audit_trail, cross_validate_severity, load_checklist
from config import DEDUCTION_SEVERITY, MODULES, REPORT_AUTHOR
from layer1_store_performance import build_store_performance
from layer2_module_analysis import build_module_analysis
from layer3_risk_level import build_risk_detail
from layer4_trend import build_trend_data
from summary_builder import build_summary
from utils import (
    filter_by_month,
    get_analysis_month,
    normalize_columns,
    parse_date_column,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
)
logger = logging.getLogger(__name__)


def read_excel(input_path):
    """Read and normalize the inspection Excel input."""
    logger.info(f"Reading inspection data: {input_path}")
    df = pd.read_excel(input_path, engine='openpyxl')
    logger.info(f"Raw data: {len(df)} rows, {len(df.columns)} columns")

    df = normalize_columns(df)

    if 'check_date' in df.columns:
        df['check_date'] = parse_date_column(df['check_date'])
    if 'deduction_value' in df.columns:
        df['deduction_value'] = pd.to_numeric(df['deduction_value'], errors='coerce').fillna(0)
    if 'report_score' in df.columns:
        df['report_score'] = pd.to_numeric(df['report_score'], errors='coerce')

    return df


def print_store_validation(df):
    """Print all unique store name + serial + dept code combinations."""
    if 'store_serial' not in df.columns:
        return []

    stores = df.drop_duplicates('store_serial')[
        ['store_name', 'store_serial', 'dept_code']
    ].sort_values('store_serial')

    print(f"\n{'='*60}")
    print(f"STORE VALIDATION ({len(stores)} stores in data)")
    print(f"{'='*60}")
    for _, row in stores.iterrows():
        print(f"  {row['store_serial']:12s} | {row.get('dept_code', ''):18s} | {row['store_name']}")
    print()

    return [
        {'store_name': str(r['store_name']), 'store_serial': str(r['store_serial']),
         'dept_code': str(r.get('dept_code', ''))}
        for _, r in stores.iterrows()
    ]


def print_diagnostics(l1, l2, l3, mapping_stats, data_quality, analysis_month):
    """Print summary diagnostics."""
    year, month = analysis_month

    print(f"\n{'='*60}")
    print(f"ANALYSIS COMPLETE — {year}-{month:02d}")
    print(f"{'='*60}")

    # Stores
    print(f"\n--- Layer 1: Store Performance ---")
    print(f"  Stores: {l1['total_stores']}")
    print(f"  Average score: {l1['monthly_average']}")
    if l1['prior_month_average'] is not None:
        print(f"  Prior month avg: {l1['prior_month_average']} (change: {l1['average_change']})")
    if l1['highest_store']:
        print(f"  Highest: {l1['highest_store']['name']} ({l1['highest_store']['score']})")
    if l1['lowest_store']:
        print(f"  Lowest: {l1['lowest_store']['name']} ({l1['lowest_store']['score']})")

    # Module mapping
    print(f"\n--- Module Mapping ---")
    print(f"  Text match: {mapping_stats.get('text_match_count', 0)}")
    print(f"  Label fallback: {mapping_stats.get('label_fallback_count', 0)}")
    print(f"  Unmapped: {mapping_stats.get('unmapped_count', 0)}")
    if mapping_stats.get('avg_match_score'):
        print(f"  Avg match score: {mapping_stats['avg_match_score']}")

    # Top 3 risk modules
    print(f"\n--- Top 3 Risk Modules (by deduction) ---")
    for m in l2.get('top_3_modules', []):
        print(f"  Module {m['module_id']}: {m['module_name_cn']} — "
              f"{m['issue_count']} issues, {m['total_deduction']} deductions")

    # S items
    print(f"\n--- S Items (Critical) ---")
    print(f"  Count: {l3['s_items_count']}")
    for s in l3.get('s_items_detail', []):
        desc = s['description'][:80] if s['description'] else '(no description)'
        print(f"  - {s['store_name']} | {s['module_cn']} | {desc}")

    # Data quality
    print(f"\n--- Data Quality ---")
    print(f"  Empty descriptions: {data_quality.get('empty_descriptions_pct', 'N/A')}")
    if data_quality.get('unmapped_labels'):
        print(f"  Unmapped labels: {data_quality['unmapped_labels']}")
    if data_quality.get('severity_mismatches', 0) > 0:
        print(f"  Severity mismatches: {data_quality['severity_mismatches']}")
    if data_quality.get('anomalous_scores'):
        print(f"  Anomalous scores: {len(data_quality['anomalous_scores'])}")
    print()


def main():
    parser = argparse.ArgumentParser(description='QA Store Audit v3 Analysis')
    parser.add_argument('--input', '-i', required=True, help='Path to inspection Excel (.xlsx)')
    parser.add_argument('--checklist', '-c', help='Path to QA checklist Excel (.xlsx, optional)')
    parser.add_argument('--month', '-m', help='Analysis month YYYY-MM (auto-detect if omitted)')
    parser.add_argument('--output-dir', '-o', default=None, help='Output directory')

    args = parser.parse_args()

    if not os.path.exists(args.input):
        logger.error(f"Input file not found: {args.input}")
        sys.exit(1)

    script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    output_dir = Path(args.output_dir) if args.output_dir else script_dir.parent / 'output'
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Read inspection data
    df = read_excel(args.input)

    # 2. Read checklist (optional)
    checklist_df = None
    if args.checklist and os.path.exists(args.checklist):
        logger.info(f"Loading QA checklist: {args.checklist}")
        checklist_df = load_checklist(args.checklist)
    else:
        logger.info("No checklist provided — using Label fallback mapping only")

    # 3. Store validation
    store_list = print_store_validation(df)

    # 4. Module assignment
    df = assign_modules(df, checklist_df)

    # 5. Severity cross-validation
    df = cross_validate_severity(df)

    # 6. Determine analysis month
    analysis_month, prior_month = get_analysis_month(df, args.month)
    year, month = analysis_month
    prior_year, prior_month_num = prior_month

    # Create month-specific output dir
    month_output = output_dir / f"qa-analysis-{year}-{month:02d}"
    month_output.mkdir(parents=True, exist_ok=True)

    # 7. Filter current month
    current_df = filter_by_month(df, year, month)
    total_stores = current_df['store_serial'].nunique() if 'store_serial' in current_df.columns else 0

    logger.info(f"Analysis month: {year}-{month:02d}, Stores: {total_stores}")

    # Check prior month
    prior_df = filter_by_month(df, prior_year, prior_month_num)
    prior_str = f"{prior_year}-{prior_month_num:02d}" if not prior_df.empty else 'N/A'
    print(f"Analysis month: {year}-{month:02d}, Stores: {total_stores}, Prior month: {prior_str}")

    # 8. Run layers
    logger.info("Running Layer 1: Store Performance...")
    l1 = build_store_performance(current_df, df, analysis_month, prior_month, month_output)

    # Use only latest inspection rows for module and risk analysis (consistent with FILE 1)
    latest_df = l1.get('latest_rows', current_df)
    latest_stores = l1.get('total_stores', total_stores)

    logger.info("Running Layer 2: Module Analysis...")
    l2 = build_module_analysis(latest_df, latest_stores, month_output)

    logger.info("Running Layer 3: Risk Detail...")
    l3 = build_risk_detail(latest_df, month_output)

    logger.info("Running Layer 4: Trend Data...")
    l4 = build_trend_data(df, month_output)

    # 9. Write audit trail
    logger.info("Building module mapping audit trail...")
    audit_df = build_audit_trail(current_df)
    audit_df.to_csv(month_output / '07_module_mapping_audit.csv', index=False, encoding='utf-8-sig')
    logger.info(f"Wrote 07_module_mapping_audit.csv ({len(audit_df)} unique mappings)")

    # 10. Compute mapping stats
    mapping_stats = {
        'checklist_used': checklist_df is not None,
        'text_match_count': int((current_df['match_method'] == 'text_match').sum()) if 'match_method' in current_df.columns else 0,
        'label_fallback_count': int((current_df['match_method'] == 'label_fallback').sum()) if 'match_method' in current_df.columns else 0,
        'unmapped_count': int((current_df['match_method'] == 'unmapped').sum()) if 'match_method' in current_df.columns else 0,
        'avg_match_score': round(current_df[current_df['match_method'] == 'text_match']['match_score'].mean(), 3) if 'match_score' in current_df.columns and (current_df['match_method'] == 'text_match').any() else None,
        'severity_mismatches': int(current_df['severity_mismatch'].sum()) if 'severity_mismatch' in current_df.columns else 0,
    }

    # 11. Data quality
    unmapped_labels = current_df[current_df['module_id'] == 0]['label'].unique().tolist() if 'label' in current_df.columns else []
    empty_desc_count = (current_df['issue_description'].fillna('').astype(str).str.strip().isin(['', 'nan'])).sum() if 'issue_description' in current_df.columns else 0
    total_rows = len(current_df)

    anomalous_scores = []
    if l1.get('anomalous_count', 0) > 0:
        store_df = l1.get('store_df', pd.DataFrame())
        anom = store_df[store_df['anomaly'] == True] if not store_df.empty else pd.DataFrame()
        for _, r in anom.iterrows():
            anomalous_scores.append({
                'store_name': r['store_name'],
                'score': float(r['report_score']),
                'date': r['check_date'],
            })

    data_quality = {
        'unmapped_labels': unmapped_labels,
        'mapping_conflicts': [],
        'anomalous_scores': anomalous_scores,
        'empty_descriptions_pct': f"{round(empty_desc_count / max(total_rows, 1) * 100, 1)}%",
        'severity_mismatches': mapping_stats['severity_mismatches'],
    }

    # 12. Write summary JSON
    store_validation = {
        'stores_in_data': store_list,
        'note': 'Store names sourced directly from empapp export, not generated',
    }

    summary = build_summary(l1, l2, l3, l4, analysis_month, prior_month,
                           mapping_stats, data_quality, store_validation)

    json_path = month_output / '00_summary.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, default=str)
    logger.info(f"Wrote {json_path}")

    # 13. Print diagnostics
    print_diagnostics(l1, l2, l3, mapping_stats, data_quality, analysis_month)

    print(f"Output files in: {month_output}/")
    for f in sorted(month_output.iterdir()):
        if f.is_file():
            print(f"  {f.name:40s} ({f.stat().st_size:,} bytes)")


if __name__ == '__main__':
    main()
