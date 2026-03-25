#!/usr/bin/env python3
"""Generate synthetic test data for QA Store Audit v3 pipeline validation.
Produces BOTH inspection xlsx AND checklist xlsx with realistic fuzzy-matchable text.
"""

import os
import random

import pandas as pd

random.seed(42)

STORES = [
    ('US00001', 'LKUS00000001', 'Times Square', 'New York City', 'Area 1', 'Pickup'),
    ('US00002', 'LKUS00000002', 'Herald Square', 'New York City', 'Area 1', 'Relax'),
    ('US00003', 'LKUS00000003', 'Bryant Park', 'New York City', 'Area 1', 'Pickup'),
    ('US00004', 'LKUS00000004', 'Wall Street', 'New York City', 'Area 2', 'Pickup'),
    ('US00005', 'LKUS00000005', 'Midtown East', 'New York City', 'Area 2', 'Pickup'),
    ('US00006', 'LKUS00000006', '28th & 6th', 'New York City', 'Area 2', 'Relax'),
    ('US00007', 'LKUS00000007', 'Penn Station', 'New York City', 'Area 3', 'Pickup'),
    ('US00008', 'LKUS00000054', 'JFK Terminal 1', 'New York City', 'Area 3', 'Pickup'),
    ('US00009', 'LKUS00000009', 'Columbus Circle', 'New York City', 'Area 1', 'Relax'),
    ('US00010', 'LKUS00000010', 'Union Square', 'New York City', 'Area 2', 'Pickup'),
]

# Checklist: clause number, bullet text, module_id
CHECKLIST_ITEMS = [
    # Module 1 - 证照文件记录
    ('1.1.1', 'Store has valid food service license prominently displayed', 1),
    ('1.1.2', 'Business registration certificate is current and accessible', 1),
    ('1.2.1', 'All required government permits are posted in customer-visible area', 1),
    ('1.2.2', 'License renewal dates are tracked and updated before expiration', 1),
    ('1.3.1', 'Government inspection records are maintained on file', 1),
    # Module 2 - 员工健康与个人卫生
    ('2.1.1', 'All food handlers have valid health certificates on file', 2),
    ('2.1.2', 'Employee health screening records are up to date', 2),
    ('2.2.1', 'Staff demonstrate proper handwashing technique and frequency', 2),
    ('2.2.2', 'Handwashing station is fully stocked with soap and paper towels', 2),
    ('2.3.1', 'Employees wear clean uniforms and proper hair restraints', 2),
    ('2.3.2', 'Disposable gloves are used during food preparation and changed between tasks', 2),
    ('2.3.3', 'Blue bandages are available and used for any cuts or wounds', 2),
    # Module 3 - 供应商管理
    ('3.1.1', 'All food supplies are from approved suppliers on the vendor list', 3),
    ('3.1.2', 'Supplier certifications and quality documents are on file', 3),
    ('3.2.1', 'Incoming goods are inspected and documented upon delivery', 3),
    # Module 4 - 交叉污染防控
    ('4.1.1', 'Raw and cooked food items are stored separately to prevent cross contamination', 4),
    ('4.1.2', 'Cutting boards and utensils are designated for specific food types', 4),
    ('4.2.1', 'Materials are stored in designated locations according to specification', 4),
    ('4.2.2', 'Utensils are properly stored and maintained to prevent contamination', 4),
    ('4.3.1', 'Measures are in place to avoid falling foreign objects into food', 4),
    # Module 5 - 清洁卫生
    ('5.1.1', 'Cleaning and disinfection procedures are followed according to schedule', 5),
    ('5.1.2', 'Cleaning schedule is posted and signed off daily', 5),
    ('5.2.1', 'Equipment and food processing utensils are cleaned after each use', 5),
    ('5.2.2', 'Operation stands, shelves, cabinets and water sinks are clean and orderly', 5),
    ('5.3.1', 'Food processing area is maintained in clean and sanitary condition', 5),
    ('5.3.2', 'Floors, walls and ceilings in food areas are clean and in good repair', 5),
    ('5.4.1', 'Customer area tables and seating are clean and well-maintained', 5),
    ('5.4.2', 'Trash cans are emptied regularly and lids are in working condition', 5),
    # Module 6 - 产品与有效期管理
    ('6.1.1', 'Temperature monitoring devices are installed in all refrigerators and freezers', 6),
    ('6.1.2', 'Temperature logs are completed and recorded at required intervals', 6),
    ('6.2.1', 'Product storage conditions meet specification requirements', 6),
    ('6.2.2', 'Food items are stored using FIFO method with proper date labels', 6),
    ('6.3.1', 'Expiration dates are checked daily and expired items are removed', 6),
    ('6.3.2', 'Food preparation follows standard operating specifications', 6),
    ('6.4.1', 'Unqualified goods are segregated and properly documented', 6),
    # Module 7 - 设备设施维护
    ('7.1.1', 'Equipment is inspected regularly and maintenance records are kept', 7),
    ('7.1.2', 'Facility structures are inspected and maintained in good condition', 7),
    ('7.2.1', 'Lighting fixtures in food preparation areas are protected and functional', 7),
    ('7.2.2', 'Refrigeration equipment is functioning within proper temperature ranges', 7),
    # Module 8 - 化学品管理
    ('8.1.1', 'All chemicals are properly labeled and stored in designated area', 8),
    ('8.1.2', 'Chemical storage area is separate from food preparation and storage areas', 8),
    ('8.2.1', 'MSDS sheets are available for all chemicals used on premises', 8),
    # Module 9 - 虫害防控
    ('9.1.1', 'Pest control service contract is current with licensed provider', 9),
    ('9.1.2', 'Pest control facilities including traps are properly maintained', 9),
    ('9.2.1', 'No signs of insect or rodent activity observed during inspection', 9),
    ('9.2.2', 'All entry points are sealed to prevent pests from entering', 9),
    # Module 10 - 饮用水与管道系统
    ('10.1.1', 'Water filtration device is installed and filter is changed on schedule', 10),
    ('10.1.2', 'Potable water supply is adequate for all operational needs', 10),
    ('10.2.1', 'Water sinks and pipes are in good condition without leaks', 10),
    ('10.2.2', 'Grease traps, residue traps and sewer lines are cleaned regularly', 10),
    ('10.3.1', 'Proper air gaps are maintained in plumbing connections', 10),
    # Module 11 - 工作场所安全
    ('11.1.1', 'Workplace safety procedures are posted and employees are trained', 11),
    ('11.1.2', 'First aid kit is stocked and accessible', 11),
    ('11.2.1', 'Fire extinguisher is inspected and within service date', 11),
    # Module 12 - 门店稽核管理
    ('12.1.1', 'Store audit management procedures and checklists are followed', 12),
    ('12.1.2', 'Previous audit findings have been addressed and documented', 12),
    ('12.2.1', 'Corrective action records are maintained for all identified issues', 12),
]

# Labels used in empapp (mapping to modules via fallback)
LABELS_BY_MODULE = {
    1: ['Licenses and certificates', 'License Documents', 'Government inspection'],
    2: ["Employees' Health", 'Personal Hygiene', 'Personal certificate',
        'Disposable Gloves and Blue Bandages', 'Handwashing Standards'],
    3: ['Approved Goods'],
    4: ['Material Storage Location Specification', 'Storage/Maintenance of Utensils',
        'Cross Contamination', 'Avoidance of Falling Foreign Objects'],
    5: ['Procedures for Cleaning and Disinfection', 'Equipment and Food Processing Utensils',
        'Operation Stand, Shelf, Cabinet and Water Sink', 'Food Processing Area',
        'Trash Cans', 'Customer Area'],
    6: ['Devices for Monitoring the Temperatures of All Refrigerators and Freezers',
        'Product Storage Conditions', 'Expiration Date', 'Food',
        'Operating Specifications', 'Storage and Inventory Transfer of Food', 'Unqualified Goods'],
    7: ['Equipment Inspection and Maintenance', 'Facility Inspection and Maintenance', 'Lights'],
    8: ['Chemical Mark/Storage'],
    9: ['No Sign of Insect Pests', 'Insect Pest Control Facilities & Suppliers', 'No pests enter'],
    10: ['Water Sinks & Pipes', 'Grease traps&Residue traps&Sewer lines',
         'Water Filtration Device', 'Potable Water Supply is Adequate'],
    11: ['Workplace Safety'],
    12: ['Requirements'],
}

DEDUCTION_TYPES = ['Key', 'Important', 'General', 'Slight', 'Other']
DEDUCTION_VALUES = {'Key': [-5, 0], 'Important': [-5], 'General': [-2], 'Slight': [-1], 'Other': [0]}

CHECK_CATEGORIES = ['Store food safety self-check', 'Store food safety audit', 'Area food safety Check']
CHECKERS = ['Zhang Wei', 'Li Ming', 'Wang Fang', 'Chen Yu']

ISSUE_DESCRIPTIONS = [
    'Food item past expiration date found in cold storage area',
    'Temperature log not completed for main refrigerator today',
    'Handwashing station missing soap dispenser refill',
    'Floor not clean under the sink area, standing water observed',
    'Ice machine leaking water, needs repair by maintenance team',
    'Chemical storage bottles not properly labeled with contents',
    'Pest trap behind counter not checked in 30+ days',
    'Employee health certificate expired last month',
    'Raw chicken stored above ready-to-eat items in refrigerator',
    'Water filter replacement is overdue by two weeks',
    'Trash can lid broken and does not close properly',
    'Operating license not displayed in customer visible area',
    'Gloves not worn during food preparation observed',
    'Food storage containers not labeled with preparation date',
    'Grease trap cleaning overdue per maintenance schedule',
    'Light fixture cover cracked in food prep area',
    'No air gap found in plumbing connection under three-compartment sink',
    'Food items stored directly on floor in dry storage room',
    'Personal items found in food preparation area',
    'Freezer temperature reading above -18C acceptable range',
    'Cutting board has deep grooves and needs replacement',
    'Mold observed on ceiling tiles near ventilation duct',
    '',  # Some entries have no description
]


def _make_check_items_text(clause, bullet_text):
    """Create a slightly modified version of the bullet text for Check items.
    This simulates how empapp might store the check item differently from the checklist.
    """
    variations = [
        lambda t: t,  # exact match
        lambda t: t.rstrip('.') + ' is verified',  # append
        lambda t: '(G) ' + t,  # add severity prefix
        lambda t: '(L) ' + t,
        lambda t: t[:len(t)//2+10],  # truncate
        lambda t: t.replace('is ', 'are ').replace('are ', 'is '),  # minor word change
    ]
    fn = random.choice(variations)
    return fn(bullet_text)


def generate_inspection_data():
    """Generate synthetic inspection xlsx with realistic Check items text."""
    rows = []
    checklist_counter = 1000

    for month_offset in [1, 0]:  # Feb (prior), March (current)
        mo = 3 - month_offset

        for store in STORES:
            serial, dept, name, city, area, level = store
            num_inspections = random.choice([1, 2])

            for insp in range(num_inspections):
                checklist_counter += 1
                cl_num = f'CK-2026-{checklist_counter}'
                check_cat = random.choice(CHECK_CATEGORIES)
                checker = random.choice(CHECKERS)
                day = random.randint(1, 28)
                check_date = f'2026-{mo:02d}-{day:02d}'
                gen_time = f'{check_date} {random.randint(8,18):02d}:{random.randint(0,59):02d}:00'
                base_score = random.randint(70, 98)
                num_items = random.randint(5, 15)

                for _ in range(num_items):
                    # Pick a random checklist item
                    clause, bullet, mod_id = random.choice(CHECKLIST_ITEMS)
                    # Pick a label from the same module
                    labels = LABELS_BY_MODULE.get(mod_id, ['Requirements'])
                    label = random.choice(labels)

                    # Generate check items text (slightly different from checklist)
                    check_text = _make_check_items_text(clause, bullet)

                    deduction_type = random.choices(
                        DEDUCTION_TYPES, weights=[2, 5, 30, 40, 23], k=1
                    )[0]
                    deduction_value = random.choice(DEDUCTION_VALUES[deduction_type])

                    # Add severity prefix to check_text sometimes
                    sev_code = {'Key': 'S', 'Important': 'M', 'General': 'G', 'Slight': 'L'}.get(deduction_type)
                    if sev_code and random.random() < 0.3:
                        check_text = f'({sev_code}) {check_text}'

                    desc = ''
                    if deduction_value != 0:
                        desc = random.choice([d for d in ISSUE_DESCRIPTIONS if d])
                    else:
                        desc = random.choice(ISSUE_DESCRIPTIONS)

                    rows.append({
                        'City': city,
                        'Operational Management Area': area,
                        'Store serial number': serial,
                        'Department code': dept,
                        'Store name': name,
                        'Checklist number': cl_num,
                        'Check category': check_cat,
                        'Check items': check_text,
                        'Opportunity point description': desc if desc else None,
                        'Label': label,
                        'Opportunity point value': deduction_value,
                        'Deduction type': deduction_type,
                        'Check route': 'On-site check',
                        'checker': checker,
                        'Checker position': 'QA Inspector',
                        'Voiding Approval Position': None,
                        'Voiding Submission Time': None,
                        'Voiding Approval Time': None,
                        'Report score': base_score,
                        'Store level': level,
                        'Check date': check_date,
                        'Check report generation time': gen_time,
                    })

    # Add anomalous negative score
    rows.append({
        'City': 'New York City', 'Operational Management Area': 'Area 1',
        'Store serial number': 'US00001', 'Department code': 'LKUS00000001',
        'Store name': 'Times Square', 'Checklist number': 'CK-2026-9999',
        'Check category': 'US Store Food Safety Audit',
        'Check items': 'Full audit comprehensive check', 'Opportunity point description': None,
        'Label': 'Requirements', 'Opportunity point value': 0, 'Deduction type': 'Other',
        'Check route': 'On-site check', 'checker': 'External Auditor',
        'Checker position': 'Senior Auditor',
        'Voiding Approval Position': None, 'Voiding Submission Time': None,
        'Voiding Approval Time': None,
        'Report score': -192, 'Store level': 'Pickup',
        'Check date': '2026-03-20', 'Check report generation time': '2026-03-20 14:30:00',
    })

    return pd.DataFrame(rows)


def generate_checklist_data():
    """Generate QA checklist xlsx."""
    rows = []
    for clause, bullet, mod_id in CHECKLIST_ITEMS:
        from config import MODULES
        mod = MODULES.get(mod_id, {})
        rows.append({
            'Clause Number': clause,
            'Bullet Points': bullet,
            'Module': mod.get('cn', ''),
            'Module EN': mod.get('en', ''),
        })
    return pd.DataFrame(rows)


if __name__ == '__main__':
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'analyze'))

    output_dir = os.path.join(os.path.dirname(__file__), 'sample-data')
    os.makedirs(output_dir, exist_ok=True)

    insp_df = generate_inspection_data()
    insp_path = os.path.join(output_dir, 'test_inspection_v3.xlsx')
    insp_df.to_excel(insp_path, index=False, engine='openpyxl')
    print(f"Inspection data: {len(insp_df)} rows → {insp_path}")
    print(f"  Stores: {insp_df['Store serial number'].nunique()}")
    print(f"  Months: {sorted(insp_df['Check date'].str[:7].unique())}")

    checklist_df = generate_checklist_data()
    cl_path = os.path.join(output_dir, 'test_checklist_v3.xlsx')
    checklist_df.to_excel(cl_path, index=False, engine='openpyxl')
    print(f"Checklist: {len(checklist_df)} bullet points → {cl_path}")
    print(f"  Modules: {checklist_df['Module'].nunique()}")
