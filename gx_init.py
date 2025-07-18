from pathlib import Path
import great_expectations as gx

# Initialize context
context = gx.get_context()

# Check if "local_filesystem" already exists
existing = [ds["name"] for ds in context.list_datasources()]
if "local_filesystem" not in existing:
    context.sources.add_pandas_filesystem(
        name="local_filesystem",
        base_directory=Path("data/incoming").resolve()
    )
    print("✅ Added 'local_filesystem' datasource.")
else:
    print("ℹ️ 'local_filesystem' already exists.")

# Verify
print("\n✅ Datasources configured:")
for ds in context.list_datasources():
    print(f"- Name: {ds['name']}, Type: {ds['type']}")