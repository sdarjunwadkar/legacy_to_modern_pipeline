from pathlib import Path
import great_expectations as gx

context = gx.get_context()

# Access the registered fluent datasource
datasource = context.datasources["local_filesystem"]

# Define an asset by matching files in the folder using regex
asset = datasource.add_csv_asset(
    name="test_file_asset",
    batching_regex=r"test_file\.csv"
)

# Build a batch request from the asset
batch_request = asset.build_batch_request()

# Create or load suite
suite_name = "test_suite"
try:
    suite = context.get_expectation_suite(suite_name)
except gx.exceptions.DataContextError:
    suite = context.add_expectation_suite(suite_name)

# Run validations
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite=suite
)

validator.expect_column_values_to_not_be_null("id")
validator.expect_column_values_to_be_in_type_list("id", ["int"])
validator.expect_column_values_to_be_in_type_list("value", ["str"])

validator.save_expectation_suite()
print(f"✅ Expectation suite '{suite_name}' created successfully.")