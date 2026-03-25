"""
Configuration constants for QA Store Audit v3.
Module definitions, clause-based mapping, severity, attribution keywords.
"""

# ---------------------------------------------------------------------------
# V3 Module definitions (from QA checklist chapter structure)
# Key: int module_id (1-12), matches clause number first digit
# ---------------------------------------------------------------------------
MODULES = {
    1:  {'cn': '证照文件记录',         'en': 'Document Record'},
    2:  {'cn': '员工健康与个人卫生',    'en': "Employees' Health & Personal Hygiene"},
    3:  {'cn': '供应商管理',           'en': 'Approved Supplier'},
    4:  {'cn': '交叉污染防控',         'en': 'Prevention of Cross-contamination'},
    5:  {'cn': '清洁卫生',             'en': 'Sanitation and Hygiene'},
    6:  {'cn': '产品与有效期管理',      'en': 'Product Procedures & Expiry Date Management'},
    7:  {'cn': '设备设施维护',         'en': 'Maintenance of Equipment and Facilities'},
    8:  {'cn': '化学品管理',           'en': 'Chemicals'},
    9:  {'cn': '虫害防控',             'en': 'Pests Control'},
    10: {'cn': '饮用水与管道系统',      'en': 'Potable Water, Pipes & Water Systems'},
    11: {'cn': '工作场所安全',         'en': 'Workplace Safety'},
    12: {'cn': '门店稽核管理',         'en': 'Store Audit Management Procedures'},
}

MODULE_ORDER = list(range(1, 13))

# ---------------------------------------------------------------------------
# Fallback: Label string → module ID (used ONLY when fuzzy text match fails)
# ---------------------------------------------------------------------------
LABEL_TO_MODULE_FALLBACK = {
    # Module 1 - 证照文件记录
    'Licenses and certificates': 1,
    'License Documents': 1,
    'Government inspection': 1,
    # Module 2 - 员工健康与个人卫生
    "Employees' Health": 2,
    'Personal Hygiene': 2,
    'Personal certificate': 2,
    'Disposable Gloves and Blue Bandages': 2,
    'Handwashing Standards': 2,
    # Module 3 - 供应商管理
    'Approved Goods': 3,
    # Module 4 - 交叉污染防控
    'Material Storage Location Specification': 4,
    'Storage/Maintenance of Utensils': 4,
    'Cross Contamination': 4,
    'Avoidance of Falling Foreign Objects': 4,
    # Module 5 - 清洁卫生
    'Procedures for Cleaning and Disinfection': 5,
    'Equipment and Food Processing Utensils': 5,
    'Operation Stand, Shelf, Cabinet and Water Sink': 5,
    'Food Processing Area': 5,
    'Trash Cans': 5,
    'Customer Area': 5,
    # Module 6 - 产品与有效期管理
    'Devices for Monitoring the Temperatures of All Refrigerators and Freezers': 6,
    'Product Storage Conditions': 6,
    'Expiration Date': 6,
    'Food': 6,
    'Operating Specifications': 6,
    'Storage and Inventory Transfer of Food': 6,
    'Unqualified Goods': 6,
    # Module 7 - 设备设施维护
    'Equipment Inspection and Maintenance': 7,
    'Facility Inspection and Maintenance': 7,
    'Lights': 7,
    # Module 8 - 化学品管理
    'Chemical Mark/Storage': 8,
    # Module 9 - 虫害防控
    'No Sign of Insect Pests': 9,
    'Insect Pest Control Facilities & Suppliers': 9,
    'No pests enter': 9,
    # Module 10 - 饮用水与管道系统
    'Water Sinks & Pipes': 10,
    'Grease traps&Residue traps&Sewer lines': 10,
    'Water Filtration Device': 10,
    'Potable Water Supply is Adequate': 10,
    # Module 11 - 工作场所安全
    'Workplace Safety': 11,
    # Module 12 - 门店稽核管理
    'Requirements': 12,
}

# ---------------------------------------------------------------------------
# Fuzzy matching thresholds
# ---------------------------------------------------------------------------
FUZZY_MATCH_THRESHOLD = 0.55
FUZZY_MATCH_HIGH_CONFIDENCE = 0.80

# ---------------------------------------------------------------------------
# Severity prefix in Check items text
# ---------------------------------------------------------------------------
SEVERITY_PREFIX_MAP = {
    '(S)': 'S',
    '(M)': 'M',
    '(L)': 'L',
}

# ---------------------------------------------------------------------------
# Deduction type → severity mapping (unchanged from v1)
# ---------------------------------------------------------------------------
DEDUCTION_SEVERITY = {
    'Key': {'label_cn': 'S项', 'code': 'S', 'level': '关键项', 'description': 'Critical'},
    'Important': {'label_cn': 'M项', 'code': 'M', 'level': '重要项', 'description': 'Important'},
    'General': {'label_cn': 'G项', 'code': 'G', 'level': '一般项', 'description': 'General'},
    'Slight': {'label_cn': 'L项', 'code': 'L', 'level': '轻微项', 'description': 'Slight'},
    'Other': {'label_cn': '其他', 'code': 'O', 'level': '其他', 'description': 'Other'},
}

SEVERITY_ORDER = ['S', 'M', 'G', 'L', 'O']

# ---------------------------------------------------------------------------
# CAPA keyword-based responsibility attribution
# ---------------------------------------------------------------------------
ATTRIBUTION_KEYWORDS = {
    '机修': [
        'broken', 'leaking', 'leak', 'not working', 'needs repair', 'malfunction',
        'damaged', 'out of order', 'defective', 'faulty', 'replace',
        'maintenance', 'repair', 'fix', 'crack', 'missing part', 'machine',
        '坏', '漏', '维修',
    ],
    '营建': [
        'no air gap', 'construction', 'plumbing', 'structural', 'install',
        'pipe', 'drain', 'grease trap', 'sewer', 'ventilation', 'ceiling',
        'wall', 'floor damage', '管道', '施工', '改造',
    ],
    '门店': [
        'dirty', 'expired', 'no label', 'not labeled', 'not covered',
        'standing water', 'dust', 'mold', 'missing', 'not clean', 'not posted',
        'uncovered', 'not wearing', 'no gloves', 'no hairnet',
        'temperature not recorded', 'not sanitized', 'not washed', 'improper',
        'no date', 'not stored',
    ],
}

# ---------------------------------------------------------------------------
# SLA standards
# ---------------------------------------------------------------------------
SLA_STANDARDS = {
    'S': {'days': 2, 'label': 'S项（关键项）', 'deadline': '2天'},
    'M': {'days': 7, 'label': 'M项（重要项）', 'deadline': '7天'},
    'G': {'days': 14, 'label': 'G项（一般项）', 'deadline': '14天'},
    'L': {'days': 14, 'label': 'L项（轻微项）', 'deadline': '14天'},
}

# ---------------------------------------------------------------------------
# Report metadata
# ---------------------------------------------------------------------------
REPORT_AUTHOR = '曾翔宇'
REPORT_DEPARTMENT = '质量保障部 / 基础设施部'
REPORT_COMPANY = '瑞幸咖啡北美（First Ray Holdings USA Inc.）'
