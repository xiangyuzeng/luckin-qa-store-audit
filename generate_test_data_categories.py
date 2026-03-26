#!/usr/bin/env python3
"""Generate multi-category test data for QA category comparison analysis.
Produces inspection data spanning Jan-Mar 2026 with realistic category distribution:
- January: all 3 categories (self-check, QA audit, area check)
- February: QA audit only (incomplete)
- March: self-check only
"""

import os
import random
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'analyze'))

random.seed(99)

STORES = [
    ('US00001', 'LKUS00000001', 'Times Square', 'New York City', 'Area 1', 'Pickup'),
    ('US00002', 'LKUS00000002', 'Herald Square', 'New York City', 'Area 1', 'Relax'),
    ('US00003', 'LKUS00000003', 'Bryant Park', 'New York City', 'Area 1', 'Pickup'),
    ('US00004', 'LKUS00000004', 'Wall Street', 'New York City', 'Area 2', 'Pickup'),
    ('US00005', 'LKUS00000005', 'Midtown East', 'New York City', 'Area 2', 'Pickup'),
    ('US00006', 'LKUS00000006', '28th & 6th', 'New York City', 'Area 2', 'Relax'),
    ('US00008', 'LKUS00000054', 'JFK Terminal 1', 'New York City', 'Area 3', 'Pickup'),
    ('US00009', 'LKUS00000009', 'Columbus Circle', 'New York City', 'Area 1', 'Relax'),
    ('US00010', 'LKUS00000010', 'Union Square', 'New York City', 'Area 2', 'Pickup'),
]
# Note: US00007 Penn Station NOT included (not opened yet per task spec)

from generate_test_data_v3 import CHECKLIST_ITEMS, LABELS_BY_MODULE, ISSUE_DESCRIPTIONS, _make_check_items_text

DEDUCTION_VALUES = {'Key': [-5, 0], 'Important': [-5], 'General': [-2], 'Slight': [-1], 'Other': [0]}

# Category-specific checkers
SELF_CHECK_CHECKERS = [
    ('Sarah Kim', 'Store Manager'),
    ('James Lee', 'Assistant Store Manager'),
    ('Maria Chen', 'Store Manager'),
    ('David Park', 'Store Manager'),
    ('Lisa Wang', 'Assistant Store Manager'),
]
QA_AUDIT_CHECKER = ('Yu Jiang', 'Senior QA Manager')
AREA_CHECK_CHECKERS = [
    ('Daniel Chu', 'Operation Manager'),
    ('Jung Han Liang', 'Operation Manager'),
]

# Category priority for "latest" selection (higher = preferred when same date)
CATEGORY_PRIORITY = {
    'Store food safety audit': 3,
    'Area food safety Check': 2,
    'Store food safety self-check': 1,
}


def _generate_inspection(store, category, checker, checker_pos, year, month, day,
                         checklist_counter, score_range, severity_weights):
    """Generate rows for one inspection."""
    serial, dept, name, city, area, level = store
    cl_num = f'CK-{year}-{checklist_counter}'
    check_date = f'{year}-{month:02d}-{day:02d}'
    gen_time = f'{check_date} {random.randint(8,18):02d}:{random.randint(0,59):02d}:00'
    base_score = random.randint(*score_range)

    num_items = random.randint(6, 14)
    rows = []
    for _ in range(num_items):
        clause, bullet, mod_id = random.choice(CHECKLIST_ITEMS)
        labels = LABELS_BY_MODULE.get(mod_id, ['Requirements'])
        label = random.choice(labels)
        check_text = _make_check_items_text(clause, bullet)

        deduction_type = random.choices(
            ['Key', 'Important', 'General', 'Slight', 'Other'],
            weights=severity_weights, k=1
        )[0]
        deduction_value = random.choice(DEDUCTION_VALUES[deduction_type])

        sev_code = {'Key': 'S', 'Important': 'M', 'General': 'G', 'Slight': 'L'}.get(deduction_type)
        if sev_code and random.random() < 0.3:
            check_text = f'({sev_code}) {check_text}'

        desc = ''
        if deduction_value != 0:
            desc = random.choice([d for d in ISSUE_DESCRIPTIONS if d])
        else:
            desc = random.choice(ISSUE_DESCRIPTIONS)

        rows.append({
            'City': city, 'Operational Management Area': area,
            'Store serial number': serial, 'Department code': dept,
            'Store name': name, 'Checklist number': cl_num,
            'Check category': category,
            'Check items': check_text,
            'Opportunity point description': desc if desc else None,
            'Label': label, 'Opportunity point value': deduction_value,
            'Deduction type': deduction_type, 'Check route': 'On-site check',
            'checker': checker, 'Checker position': checker_pos,
            'Voiding Approval Position': None, 'Voiding Submission Time': None,
            'Voiding Approval Time': None,
            'Report score': base_score, 'Store level': level,
            'Check date': check_date, 'Check report generation time': gen_time,
        })
    return rows


def generate():
    all_rows = []
    counter = 2000

    # === JANUARY 2026: All 3 categories ===

    # Self-check: all 9 stores, days 5-15
    for store in STORES:
        checker, pos = random.choice(SELF_CHECK_CHECKERS)
        day = random.randint(5, 15)
        counter += 1
        # Self-checks score higher (lenient: 85-98), mostly L/G items
        rows = _generate_inspection(store, 'Store food safety self-check',
                                    checker, pos, 2026, 1, day, counter,
                                    score_range=(85, 98),
                                    severity_weights=[1, 3, 25, 45, 26])
        all_rows.extend(rows)

    # QA audit: 7 stores (not all), days 16-25, Yu Jiang
    qa_stores = random.sample(STORES, 7)
    for store in qa_stores:
        day = random.randint(16, 25)
        counter += 1
        # QA audit stricter: lower scores (70-88), more S/M items
        rows = _generate_inspection(store, 'Store food safety audit',
                                    QA_AUDIT_CHECKER[0], QA_AUDIT_CHECKER[1],
                                    2026, 1, day, counter,
                                    score_range=(70, 88),
                                    severity_weights=[5, 10, 30, 35, 20])
        all_rows.extend(rows)

    # Area check: only 4 stores, days 10-20
    area_stores = random.sample(STORES, 4)
    for store in area_stores:
        checker, pos = random.choice(AREA_CHECK_CHECKERS)
        day = random.randint(10, 20)
        counter += 1
        # Area check moderate: scores 75-90
        rows = _generate_inspection(store, 'Area food safety Check',
                                    checker, pos, 2026, 1, day, counter,
                                    score_range=(75, 90),
                                    severity_weights=[3, 7, 30, 40, 20])
        all_rows.extend(rows)

    # === FEBRUARY 2026: QA audit only (incomplete) ===
    feb_stores = random.sample(STORES, 5)
    for store in feb_stores:
        day = random.randint(5, 25)
        counter += 1
        rows = _generate_inspection(store, 'Store food safety audit',
                                    QA_AUDIT_CHECKER[0], QA_AUDIT_CHECKER[1],
                                    2026, 2, day, counter,
                                    score_range=(72, 90),
                                    severity_weights=[4, 8, 30, 38, 20])
        all_rows.extend(rows)

    # === MARCH 2026: Self-check only ===
    for store in STORES:
        checker, pos = random.choice(SELF_CHECK_CHECKERS)
        day = random.randint(3, 25)
        counter += 1
        rows = _generate_inspection(store, 'Store food safety self-check',
                                    checker, pos, 2026, 3, day, counter,
                                    score_range=(84, 97),
                                    severity_weights=[1, 3, 25, 45, 26])
        all_rows.extend(rows)

    return pd.DataFrame(all_rows)


if __name__ == '__main__':
    output_dir = os.path.join(os.path.dirname(__file__), 'sample-data')
    os.makedirs(output_dir, exist_ok=True)

    df = generate()
    path = os.path.join(output_dir, 'test_category_data.xlsx')
    df.to_excel(path, index=False, engine='openpyxl')

    print(f"Generated {len(df)} rows → {path}")
    print(f"  Stores: {df['Store serial number'].nunique()}")
    print(f"  Months: {sorted(df['Check date'].str[:7].unique())}")
    print(f"\n  Category × Month breakdown:")
    for month in sorted(df['Check date'].str[:7].unique()):
        mdf = df[df['Check date'].str[:7] == month]
        print(f"    {month}:")
        for cat in sorted(mdf['Check category'].unique()):
            cdf = mdf[mdf['Check category'] == cat]
            stores = cdf['Store serial number'].nunique()
            checkers = ', '.join(cdf['checker'].unique())
            print(f"      {cat}: {stores} stores, {len(cdf)} rows, checkers: {checkers}")
