"""
Clause-based module matching engine for QA Store Audit v3.
Fuzzy text matching against QA checklist, with Label-based fallback.
"""

import logging
import re
from difflib import SequenceMatcher

import pandas as pd

from config import (
    FUZZY_MATCH_HIGH_CONFIDENCE,
    FUZZY_MATCH_THRESHOLD,
    LABEL_TO_MODULE_FALLBACK,
    MODULES,
    SEVERITY_PREFIX_MAP,
    DEDUCTION_SEVERITY,
)

logger = logging.getLogger(__name__)


def load_checklist(checklist_path):
    """Parse QA checklist xlsx into a normalized DataFrame.

    Handles flexible column names. Returns DataFrame with:
      clause, bullet_text, bullet_text_normalized, module_id
    """
    df = pd.read_excel(checklist_path, engine='openpyxl')
    df.columns = [c.strip() for c in df.columns]

    # Find clause column
    clause_col = _find_column(df, [
        'Clause Number', 'Clause', 'clause', 'Clause No.', 'clause_number',
    ])
    # Find bullet text column
    bullet_col = _find_column(df, [
        'Bullet Points', 'Bullet Point', 'bullet_point', 'Clause Contents',
        'Description', 'Check Items', 'Content',
    ])

    if clause_col is None or bullet_col is None:
        raise ValueError(
            f"Checklist must have clause and bullet columns. "
            f"Found columns: {list(df.columns)}"
        )

    result = pd.DataFrame({
        'clause': df[clause_col].astype(str).str.strip(),
        'bullet_text': df[bullet_col].astype(str).str.strip(),
    })

    # Drop empty rows
    result = result[result['bullet_text'].str.len() > 0].copy()
    result = result[result['clause'].str.len() > 0].copy()

    # Derive module_id from clause first digit
    result['module_id'] = result['clause'].apply(clause_to_module_id)

    # Normalize text for matching
    result['bullet_text_normalized'] = result['bullet_text'].apply(_normalize_text)

    logger.info(
        f"Loaded checklist: {len(result)} bullet points across "
        f"{result['module_id'].nunique()} modules"
    )
    return result


def _find_column(df, candidates):
    """Find a column by trying multiple name candidates (case-insensitive)."""
    col_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand in df.columns:
            return cand
        if cand.lower() in col_lower:
            return col_lower[cand.lower()]
    return None


def clause_to_module_id(clause):
    """Extract module number from clause string. '1.2.3'→1, '10.1'→10."""
    try:
        clause = str(clause).strip()
        first_part = clause.split('.')[0]
        mid = int(first_part)
        if 1 <= mid <= 12:
            return mid
    except (ValueError, IndexError):
        pass
    return 0


def _normalize_text(text):
    """Normalize text for fuzzy matching: lowercase, strip severity prefix, collapse whitespace."""
    if not isinstance(text, str) or not text.strip():
        return ''
    text = text.strip()
    # Strip leading severity prefix like (S), (M), (L)
    text = re.sub(r'^\([SML]\)\s*', '', text)
    text = text.lower()
    text = re.sub(r'\s+', ' ', text)
    text = text.strip(' .,;:')
    return text


def _extract_severity_prefix(text):
    """Extract severity from '(S) Some check item' → 'S'. Returns None if no prefix."""
    if not isinstance(text, str):
        return None
    text = text.strip()
    for prefix, sev in SEVERITY_PREFIX_MAP.items():
        if text.startswith(prefix):
            return sev
    return None


def _best_match(normalized_check, checklist_df, threshold):
    """Find the best matching checklist bullet for a check_items text."""
    if not normalized_check or checklist_df is None or checklist_df.empty:
        return None, 0.0

    best_score = 0.0
    best_idx = None

    for idx, row in checklist_df.iterrows():
        bullet = row['bullet_text_normalized']
        if not bullet:
            continue

        # Fast path: exact or substring match
        if normalized_check == bullet:
            return idx, 1.0
        if normalized_check in bullet or bullet in normalized_check:
            score = 0.90
        else:
            score = SequenceMatcher(None, normalized_check, bullet).ratio()

        if score > best_score:
            best_score = score
            best_idx = idx
        if score >= 0.99:
            break

    if best_score >= threshold:
        return best_idx, best_score
    return None, best_score


def assign_modules(df, checklist_df=None):
    """Assign module_id to every row in the inspection DataFrame.

    Strategy:
      1. If checklist_df provided: fuzzy match check_items → clause → module_id
      2. Fallback: LABEL_TO_MODULE_FALLBACK[label]
      3. If neither works: module_id=0 (unmapped)

    Adds columns: module_id, module_cn, module_en, matched_clause,
    match_score, match_method, match_confidence, text_severity
    """
    df = df.copy()

    # Normalize check_items for matching
    check_items_col = 'check_items' if 'check_items' in df.columns else None
    label_col = 'label' if 'label' in df.columns else None

    # Pre-compute normalized check texts
    if check_items_col:
        df['_check_normalized'] = df[check_items_col].apply(
            lambda x: _normalize_text(str(x)) if pd.notna(x) else ''
        )
        df['text_severity'] = df[check_items_col].apply(_extract_severity_prefix)
    else:
        df['_check_normalized'] = ''
        df['text_severity'] = None

    # Build match cache: normalized_text → (clause, score, method, confidence)
    match_cache = {}
    fuzzy_count = 0
    fallback_count = 0
    unmapped_count = 0

    results = []
    for idx, row in df.iterrows():
        norm_text = row['_check_normalized']
        label = str(row.get(label_col, '')) if label_col else ''

        cache_key = (norm_text, label)
        if cache_key in match_cache:
            results.append(match_cache[cache_key])
            continue

        matched_clause = None
        match_score = 0.0
        match_method = 'unmapped'
        match_confidence = 'none'
        module_id = 0

        # Try fuzzy matching against checklist
        if checklist_df is not None and norm_text:
            best_idx, score = _best_match(norm_text, checklist_df, FUZZY_MATCH_THRESHOLD)
            if best_idx is not None:
                matched_clause = checklist_df.loc[best_idx, 'clause']
                match_score = score
                module_id = checklist_df.loc[best_idx, 'module_id']
                match_method = 'text_match'
                if score >= FUZZY_MATCH_HIGH_CONFIDENCE:
                    match_confidence = 'high'
                elif score >= FUZZY_MATCH_THRESHOLD:
                    match_confidence = 'medium'
                fuzzy_count += 1

        # Fallback to label mapping
        if module_id == 0 and label in LABEL_TO_MODULE_FALLBACK:
            module_id = LABEL_TO_MODULE_FALLBACK[label]
            match_method = 'label_fallback'
            match_confidence = 'fallback'
            matched_clause = 'fallback'
            fallback_count += 1

        if module_id == 0:
            unmapped_count += 1

        result = {
            'module_id': module_id,
            'matched_clause': matched_clause,
            'match_score': round(match_score, 3),
            'match_method': match_method,
            'match_confidence': match_confidence,
        }
        match_cache[cache_key] = result
        results.append(result)

    # Apply results
    result_df = pd.DataFrame(results, index=df.index)
    for col in result_df.columns:
        df[col] = result_df[col]

    # Add module names
    df['module_cn'] = df['module_id'].apply(
        lambda mid: MODULES.get(mid, {}).get('cn', '未分类')
    )
    df['module_en'] = df['module_id'].apply(
        lambda mid: MODULES.get(mid, {}).get('en', 'Uncategorized')
    )

    # Map deduction_type to severity code
    df['severity'] = df.get('deduction_type', pd.Series(dtype=str)).map(
        lambda x: DEDUCTION_SEVERITY.get(x, {}).get('code', 'O') if pd.notna(x) else 'O'
    )

    # Drop temp column
    df = df.drop(columns=['_check_normalized'], errors='ignore')

    logger.info(
        f"Module assignment: {fuzzy_count} text_match, "
        f"{fallback_count} label_fallback, {unmapped_count} unmapped"
    )

    return df


def cross_validate_severity(df):
    """Compare text prefix severity with Deduction type severity. Adds severity_mismatch column."""
    df = df.copy()

    def _check_mismatch(row):
        text_sev = row.get('text_severity')
        deduction_sev = row.get('severity')
        if text_sev is None or pd.isna(text_sev):
            return False
        return str(text_sev) != str(deduction_sev)

    df['severity_mismatch'] = df.apply(_check_mismatch, axis=1)

    mismatch_count = df['severity_mismatch'].sum()
    if mismatch_count > 0:
        logger.warning(f"Severity cross-validation: {mismatch_count} mismatches found")

    return df


def build_audit_trail(df):
    """Build mapping audit data — one row per unique (check_items, label) combination."""
    check_col = 'check_items' if 'check_items' in df.columns else None
    label_col = 'label' if 'label' in df.columns else None

    if check_col is None:
        return pd.DataFrame()

    # Group by check_items + label
    group_cols = [c for c in [check_col, label_col] if c]
    agg_cols = {
        'module_id': 'first',
        'module_cn': 'first',
        'module_en': 'first',
        'matched_clause': 'first',
        'match_score': 'first',
        'match_method': 'first',
        'match_confidence': 'first',
        'text_severity': 'first',
        'severity': 'first',
        'severity_mismatch': 'first',
    }
    # Only include columns that exist
    agg_cols = {k: v for k, v in agg_cols.items() if k in df.columns}

    # Build named agg dict
    named_aggs = {'count': ('module_id', 'size')}
    for k, v in agg_cols.items():
        named_aggs[k] = (k, v)

    grouped = df.groupby(group_cols, dropna=False).agg(**named_aggs).reset_index()

    # Add check_items preview
    if check_col:
        grouped['check_items_preview'] = grouped[check_col].astype(str).str[:80]

    # Rename for output
    rename = {}
    if label_col:
        rename[label_col] = 'empapp_label'
    if check_col and check_col != 'check_items_preview':
        rename[check_col] = 'check_items_full'

    grouped = grouped.rename(columns=rename)

    return grouped.sort_values(['module_id', 'count'], ascending=[True, False])
