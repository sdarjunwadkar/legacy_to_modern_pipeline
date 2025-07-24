from great_expectations.data_context import get_context

# List of (old_name, new_name) tuples
suite_renames = [
    ("UTP_Project_Info_csj_list_suite", "utp_project_info_csj_list_suite"),
    ("UTP_Project_Info_comments_&_actions_suite", "utp_project_info_comments_actions_suite"),
    ("UTP_Project_Info_project_names_suite", "utp_project_info_project_names_suite"),
    ("UTP_Project_Info_project_tiers_suite", "utp_project_info_project_tiers_suite"),
    ("UTP_Project_Info_project_group_id_suite", "utp_project_info_project_group_id_suite"),
    ("UTP_Project_Info_hwy_various_suite", "utp_project_info_hwy_various_suite"),
]

context = get_context()

for old_name, new_name in suite_renames:
    print(f"🔄 Renaming {old_name} ➡️ {new_name}")
    old_suite = context.get_expectation_suite(old_name)
    old_suite.expectation_suite_name = new_name
    context.save_expectation_suite(old_suite)
    context.delete_expectation_suite(old_name)

print("\n✅ All suites renamed successfully.")