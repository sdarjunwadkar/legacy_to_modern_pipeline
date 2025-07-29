#!/bin/bash

echo "📁 Scanning Airflow DAGs for imported files..."
grep -rE "(import|from)" airflow_home/dags/

echo -e "\n🔍 Tracing all Python callables used in DAGs..."
grep -r "python_callable=" dags/

echo -e "\n📦 Searching for subprocess or os.system script calls..."
grep -rE "subprocess|os.system|python scripts/" .

echo -e "\n🧪 Tracing function definitions used as callables..."
for f in $(grep -rho "python_callable=[a-zA-Z0-9_]*" dags/ | cut -d= -f2 | sort | uniq); do
    echo -e "\n➡️ Function: $f"
    grep -r "def $f" .
done

echo -e "\n🧼 Running vulture for unused code analysis..."
if ! command -v vulture &> /dev/null; then
    echo "⚠️ vulture not installed. Run: pip install vulture"
else
    vulture . --min-confidence 80
fi
