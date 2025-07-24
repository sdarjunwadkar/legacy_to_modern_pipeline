from pathlib import Path
import great_expectations as gx

# Step 1: Load GE context
context = gx.get_context()

# Step 2: Reference fluent-style datasource
datasource = context.get_datasource("local_filesystem")

# Step 3: Add CSV asset only if it doesn't exist
asset_name = "utp_project_info_asset"
existing_asset_names = [a.name for a in datasource.assets]

# If asset doesn't exist, add Excel asset instead of CSV
if asset_name not in existing_asset_names:
    asset = datasource.add_excel_asset(
        name=asset_name,
        batching_regex=r"UTP_Project_Info\.xlsx"
    )
    print(f"🆕 Asset '{asset_name}' added.")
else:
    asset = datasource.get_asset(asset_name)
    print(f"ℹ️ Asset '{asset_name}' already exists. Using existing asset.")

# Step 4: Build BatchRequest
batch_request = asset.build_batch_request()

# Step 5: Create and run checkpoint
checkpoint = context.add_or_update_checkpoint(
    name="test_checkpoint",
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": "test_suite"
        }
    ]
)

result = checkpoint.run()

# Step 6: Print pass/fail result
print("\n✅ Validation Status:", "PASSED ✅" if result["success"] else "FAILED ❌")