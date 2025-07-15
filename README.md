# 🧱 Legacy-to-Modern Data Pipeline

This project simulates a cloud-style data pipeline (bronze-silver-gold) using local tools such as DuckDB, dbt-core, Soda Core, and Airflow/Prefect to replace legacy MS Access + Excel workflows.

## 🔧 Technologies
- Python
- DuckDB
- Soda Core
- dbt-core
- Airflow or Prefect
- Pandas
- Watchdog (file monitoring)

## 📁 Folder Structure
- `data/` – incoming files, bronze/silver/gold layers
- `dq_checks/` – data validation rules
- `dbt_project/` – dbt models
- `scripts/` – Python automation