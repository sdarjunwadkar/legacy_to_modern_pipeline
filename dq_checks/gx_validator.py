# dq_checks/gx_validator.py

from pathlib import Path
import great_expectations as gx
import re
from great_expectations.core.expectation_suite import ExpectationSuite

# Initialize GE context once
context = gx.get_context()

# Make sure the fluent-style datasource exists
datasource_name = "local_filesystem"

# Confirm the name exists
if datasource_name not in [ds["name"] for ds in context.list_datasources()]:
    raise ValueError(
        f"Datasource '{datasource_name}' not found. Available: {[ds['name'] for ds in context.list_datasources()]}"
    )

# ✅ Now fetch the actual live Datasource object
datasource = context.get_datasource(datasource_name)

if not datasource:
    datasource = context.sources.add_pandas_filesystem(
        name=datasource_name,
        base_directory=Path("data/incoming").resolve()
    )

def run_validation_for_file(filename: str, suite_name: str = "default_suite") -> bool:
    """
    Run Great Expectations validation for the given CSV file in data/incoming/.

    Returns True if validation passed, False otherwise.
    """
    print(f"🔍 Validating file: {filename}")

    # 1. Register CSV asset (only if not already exists)
    asset_name = Path(filename).stem + "_asset"
    if asset_name in datasource.assets:
        print(f"ℹ️ Asset '{asset_name}' already exists. Using it.")
        asset = datasource.assets[asset_name]
    else:
        # 1. Register CSV asset (only if not already exists)
        asset_name = Path(filename).stem + "_asset"

        existing_assets = [a.name for a in datasource.assets]
        if asset_name in existing_assets:
            print(f"ℹ️ Asset '{asset_name}' already exists. Using it.")
            asset = next(a for a in datasource.assets if a.name == asset_name)
        else:
            # Choose correct method based on file extension
            if filename.endswith(".csv"):
                asset = datasource.add_csv_asset(
                    name=asset_name,
                    batching_regex=re.escape(filename)
                )
            elif filename.endswith(".xlsx"):
                asset = datasource.add_excel_asset(
                    name=asset_name,
                    batching_regex=re.escape(filename)
                )
            else:
                raise ValueError(f"Unsupported file type: {filename}")

            print(f"✅ Registered new asset: {asset_name}")

    # 2. Create suite if it doesn't exist
    if suite_name not in context.list_expectation_suite_names():
        context.save_expectation_suite(ExpectationSuite(expectation_suite_name=suite_name))
        print(f"🆕 Created new suite: {suite_name}")

    # 3. Run validation
    checkpoint = context.add_or_update_checkpoint(
        name=f"{asset_name}_checkpoint",
        validations=[{
            "batch_request": asset.build_batch_request(),
            "expectation_suite_name": suite_name,
        }]
    )
    result = checkpoint.run()

    success = result["success"]
    print(f"\n✅ Validation Status: {'PASSED ✅' if success else 'FAILED ❌'}\n")
    return success