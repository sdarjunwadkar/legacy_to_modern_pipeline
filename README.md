# Legacy to Modern Data Pipeline

A modular data validation pipeline built with **Airflow** and **Great Expectations**, designed to monitor incoming Excel files, validate schema and content, and trigger downstream promotion actions.

---

## 📁 Project Structure

```
legacy_to_modern_pipeline/
├── dags/                         # Airflow DAGs
├── data/                         # Data folders (incoming, bronze, silver, gold)
├── dq_checks/                   # Great Expectations logic (gx_validator, schemas)
├── logs/                        # Validation logs & cache
├── scripts/                     # File watcher, trigger scripts
├── utils/                       # DAG utilities, debounce logic
├── great_expectations/         # GE suite config, expectations, checkpoints
├── tests/                       # pytest-based DAG and pipeline tests
├── requirements.txt             # Python dependencies
├── .env                         # Configuration (ALERT_ONLY_MODE, etc.)
├── .envrc                       # Auto environment setup with direnv
└── README.md
```

---

## 🚀 Features Implemented So Far

* ✅ **Airflow DAGs**

  * `file_poller_dag`: Detects changes in incoming files
  * `file_validation_dag`: Runs Great Expectations validations
  * `validation_alert_dag`: Sends alert if validation fails
  * `validation_pm_check_dag`: Escalation DAG for PMs
  * `daily_bronze_promotion_dag`: Promotes clean files to bronze layer

* ✅ **Great Expectations Integration**

  * Configured `great_expectations/` with checkpoints and expectations for all six sheets
  * Validations are triggered from within Airflow tasks

* ✅ **File Debounce Logic**

  * Change detection using hashing to avoid redundant validations

* ✅ **Testing Suite**

  * Pytest framework for validating DAG logic, file monitoring, and chaining

---

## ⚙️ Local Setup Instructions

### 1. Clone the repo and create a virtual environment

```bash
git clone https://github.com/your-username/legacy_to_modern_pipeline.git
cd legacy_to_modern_pipeline

python3 -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .venv\Scripts\activate   # Windows
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Set environment variables

Create a `.env` file in the project root:

```env
ALERT_ONLY_MODE=true
```

### 3b. Optional: Use `.envrc` with `direnv` for auto setup

To simplify environment setup on each terminal session, you can use a `.envrc` file with [`direnv`](https://direnv.net/).

First, install `direnv` (if not already installed):

```bash
brew install direnv  # macOS
# or
sudo apt install direnv  # Ubuntu/Debian
```

Create a `.envrc` file:

```bash
cp .envrc.template .envrc
direnv allow
```

Your `.envrc.template` should look like this:

```bash
# Set Airflow home directory
export AIRFLOW_HOME="$PWD/airflow_home"

# Activate the virtual environment
source .venv/bin/activate

# Custom environment variables
export ALERT_ONLY_MODE=true
```

Once set up, every time you `cd` into the project directory, `direnv` will automatically activate your virtual environment and export environment variables.

### 4. Initialize Airflow

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow db init
airflow users create \
  --username Your_Username \
  --firstname Your_Name \
  --lastname Your_Lastname \
  --role Admin \
  --email something@example.com \
  --password test123
```

### 5. Run Airflow

In one terminal:

```bash
airflow scheduler
```

In another:

```bash
airflow webserver --port 8080
```

Access Airflow UI at: [http://localhost:8080](http://localhost:8080)

---

## ✅ Next Steps

* [ ] Setup **CI/CD with GitHub Actions** for validating and promoting changes
* [ ] Integrate **email/Slack alerts** for failed validations
* [ ] Build **Silver Layer** using Stored Procedures and/or dbt
* [ ] Implement **Gold Layer transformations**
* [ ] Improve logging and metrics visibility (e.g., Prometheus, Grafana)

---

## 📌 Notes

* Data validation logic lives in `dq_checks/gx_validator.py` and `dq_checks/schemas.py`
* DAG chaining uses XCom and Airflow triggers
* Custom debounce logic is in `utils/debounce_mode.py`
* Use `scripts/trigger_file_validation_dag.py` to programmatically trigger validation

---

## 🧪 Run Tests

```bash
pytest tests/
```

---

## 🤝 Contributing

If you're interested in contributing, feel free to fork the repo and submit a pull request.

---

## 📜 License

MIT License
