import great_expectations as gx

# This creates the GE context in your project folder
context = gx.get_context(
    mode="file",
    project_root_dir="/Users/sarthakmistry/Documents/E-commerce_pipeline/include/great_expectations"
)

print("Great Expectations context created successfully!")
print(f"GE version: {gx.__version__}")