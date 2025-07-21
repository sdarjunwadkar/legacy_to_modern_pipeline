# dq_checks/schemas.py

BIGDATA_COLUMNS = {
    "District": "string",
    "Control Section Job": "string",
    "Controlling Project ID": "string",
    "Fiscal Year": "number",
    "ESTIMATED LET DATE": "datetime",
    "COUNTY": "string",
    "HIGHWAY": "string",
    "PROJECT CLASS": "string",
    "PROJECT DESCRIPTION": "string",
    "LIMITS FROM": "string",
    "LIMITS TO": "string",
    "MPO Name": "string",
    "CONSTRUCTION ESTIMATE": "number",
    "UTP Action": "string",
    "FUNDING LINE ORDER #": "number",
    "CATEGORY": "string",
    "WORK PROGRAM": "string",
    "AUTHORIZED AMOUNT": "number",
    "FIXED FUNDS": "string",
    "PID": "string",
    "FUNDING APPROVAL STATUS": "string",
    "TOLL FLAG": "string",
    "Cat 2 Total Recommened": "number",
    "Cat 4U Total Recommened": "number",
    "Cat 4R Total Recommended": "number",
    "Cat 12TTC Total Recommended": "number",
    "Cat 12CL Total Recommened": "number",
    "FUNDING GROUP NAME": "string",
    "ACTUAL LET DATE": "string",
    "LETTING ESTIMATE": "number"
}

CSJ_LIST_COLUMNS = {
    "CSJ": "string"
}

COMMENTS_COLUMNS = {
    "CSJ": "string",
    "CCSJ": "string",
    "District": "string",
    "UTP Action": "string",
    "Comments": "string",
    "Current Cat 2, 4, or 12?": "string",
    "Rec Cat 2": "string",
    "Cat 2 Recommended Amount": "number",
    "Rec Cat 4U": "string",
    "Cat 4U Recommended Amount": "number",
    "Rec Cat 4R Shift": "string",
    "Cat 4R Funding Shifts": "number",
    "Rec Cat 12TTC Shift": "string",
    "Cat 12TTC Funding Shifts": "number",
    "Rec Cat 12 CL Shifts": "string",
    "Cat 12 CL Funding Shifts": "number",
    "Rec Cat 4R New": "string",
    "Cat 4R New Funding": "number",
    "Rec Cat 12TTC New": "string",
    "Cat 12 TTC New Funding": "number",
    "Rec Cat 12CL New": "string",
    "Cat 12CL New Funding": "number",
    "Rec SWDA": "string",
    "SWDA Recommended Amount": "number",
    "DDA Reduction": "string",
    "DDA Recommended Amount": "string",  # Note: this was "Short Text", clarify if it's numeric
    "Total Other Categories Recommended": "number",
    "Total Amount Recommended (Cat 2/4/12)": "number"
}

PROJECT_NAMES_COLUMNS = {
    "CSJ": "string",
    "New Project Name": "string"
}

PROJECT_TIERS_COLUMNS = {
    "CONTROL SECTION JOB": "string",
    "Tiers": "string"
}

PROJECT_GROUP_ID_COLUMNS = {
    "Project ID": "string",
    "District": "string",
    "CSJ": "string",
    "CCSJ": "string",
    "Group Name": "string",
    "Fiscal Year": "number"
}

HWY_VARIOUS_COLUMNS = {
    "District": "string",
    "Control Section Job": "string",
    "Controlling Project ID": "string",
    "Fiscal Year": "number",
    "ESTIMATED LET DATE": "datetime",
    "COUNTY": "string",
    "HIGHWAY": "string",
    "In 2023 UTP Project Listings file": "string"
}