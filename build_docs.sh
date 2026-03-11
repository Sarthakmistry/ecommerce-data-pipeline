#!/bin/bash

# Navigate to repo root regardless of where script is called from
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

python - <<EOF
import great_expectations as gx

context = gx.get_context(
    mode="file",
    project_root_dir="include/great_expectations"
)
context.build_data_docs()
print("Data Docs built successfully!")
EOF

echo "Data Docs serving at http://localhost:8081"
echo "Press Ctrl+C to stop"
python -m http.server 8081 \
  --directory include/great_expectations/gx/uncommitted/data_docs/local_site