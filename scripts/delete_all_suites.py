# scripts/delete_all_suites.py

import great_expectations as gx

context = gx.get_context()
suites = context.list_expectation_suite_names()

for suite_name in suites:
    if "default_suite" in suite_name:
        continue  # Skip default suite if you want to keep it
    print(f"🗑 Deleting: {suite_name}")
    context.delete_expectation_suite(suite_name)

print("\n✅ Done cleaning up expectation suites.")