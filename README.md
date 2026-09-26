# 🌦️ Azure Weather Lakehouse — End-to-End Data Engineering Project

[![Azure Data Factory](https://img.shields.io/badge/Azure%20Data%20Factory-Orchestration-0078D4)](https://azure.microsoft.com/products/data-factory/)
[![ADLS Gen2](https://img.shields.io/badge/ADLS%20Gen2-Data%20Lake-0078D4)](https://azure.microsoft.com/products/storage/data-lake-storage/)
[![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-FF3621)](https://www.databricks.com/)
[![PySpark](https://img.shields.io/badge/PySpark-Data%20Transformation-E25A1C)](https://spark.apache.org/docs/latest/api/python/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Silver%20Layer-00ADD8)](https://delta.io/)
[![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-CI%2FCD-2088FF)](https://github.com/features/actions)
[![WeatherAPI](https://img.shields.io/badge/WeatherAPI-Source-4B8BBE)](https://www.weatherapi.com/)

## 🎯 Overview

This project implements an end-to-end **Data Engineering pipeline** that ingests weather forecast data from **WeatherAPI**, stores the raw JSON payload in **Azure Data Lake Storage Gen2**, and processes it with **Databricks + PySpark** into a structured **Delta Lake Silver layer**.

The workflow is orchestrated with **Azure Data Factory (ADF)** and follows a simplified **Medallion Architecture**:

```text
WeatherAPI
    │
    ▼
Azure Data Factory
    │
    ▼
ADLS Gen2
Bronze / Raw JSON
    │
    ▼
Databricks Job
PySpark Transformation
    │
    ▼
Delta Lake
Silver Layer
```

The project also includes a complete **DEV / TEST / PROD deployment strategy**, with:

- Azure Data Factory deployments through **ARM templates**;
- Azure authentication from GitHub through **OIDC**;
- Databricks jobs managed as code with **Declarative Automation Bundles**;
- GitHub Actions workflows for both Azure and Databricks deployments;
- secrets kept outside source control.

The project was designed as a portfolio-oriented implementation of a realistic cloud data-engineering workflow, while keeping infrastructure usage minimal.

---

## 📐 Overall Architecture

```text
                         ┌───────────────────┐
                         │    WeatherAPI     │
                         └─────────┬─────────┘
                                   │ REST
                                   ▼
                         ┌───────────────────┐
                         │ Azure Data Factory│
                         │   Orchestration   │
                         └─────────┬─────────┘
                                   │
                                   ▼
                         ┌───────────────────┐
                         │   ADLS Gen2       │
                         │ Bronze / Raw JSON │
                         └─────────┬─────────┘
                                   │ HTTPS + SAS
                                   ▼
                         ┌───────────────────┐
                         │    Databricks     │
                         │ PySpark Notebook  │
                         └─────────┬─────────┘
                                   │
                                   ▼
                         ┌───────────────────┐
                         │    Delta Lake     │
                         │   Silver Layer    │
                         └───────────────────┘
```

### Environment strategy

The project uses three logical environments:

| Environment | Azure Data Factory | ADLS | Databricks target |
|---|---|---|---|
| DEV | `adf-weather-lakehouse-dev` | DEV storage account | `weather_dev.silver.weather` |
| TEST | `adf-weather-lakehouse-test` | TEST storage account | `weather_test.silver.weather` |
| PROD | `adf-weather-lakehouse-prod` | PROD storage account | `weather_prod.silver.weather` |

Because Databricks Free Edition is used, DEV / TEST / PROD are represented inside the same workspace through separate jobs, parameters, and catalogs instead of separate Databricks workspaces.

---

## ⚙️ Azure Data Factory Pipeline

The main pipeline is:

```text
pl_ingest_weather_bronze
```

The orchestration flow is:

```text
Set_Bronze_File_Name
        │
        ▼
Copy_WeatherAPI_to_Bronze
        │
        ▼
Run_Databricks_Job
        │
        ▼
Wait_For_Databricks
        │
        ├── Wait_10_seconds
        │
        ├── Check_Databricks_Run_Status
        │
        ├── Set_Databricks_State
        │
        └── Set_Databricks_Result
        │
        ▼
Check_Databricks_Result
        │
        ├── Success → Pipeline completed
        │
        └── Failure → Fail_Databricks_Job
```

### 1. `Set_Bronze_File_Name`

The first activity creates a unique filename for every ingestion.

ADF expression:

```text
@concat(
    'weather_',
    formatDateTime(utcNow(),'yyyyMMdd_HHmmss'),
    '.json'
)
```

Example output:

```text
weather_20260926_153000.json
```

This filename is stored in the pipeline variable:

```text
bronze_file_name
```

The same filename is later passed to Databricks so that the transformation job knows exactly which Bronze file belongs to the current pipeline execution.

---

## 📥 Bronze Ingestion — WeatherAPI to ADLS Gen2

The `Copy_WeatherAPI_to_Bronze` activity calls WeatherAPI through an ADF REST dataset.

The API request follows this pattern:

```text
v1/forecast.json?key=<API_KEY>&q=Naples&days=1
```

The API key is not stored in source control. It is provided through the ADF pipeline parameter:

```text
weather_api_key
```

The raw response is copied without transformation into:

```text
data/
└── bronze/
    └── weather/
        └── weather_YYYYMMDD_HHMMSS.json
```

This preserves the original API payload and separates ingestion from transformation.

### Why keep a raw Bronze layer?

The Bronze layer makes it possible to:

- preserve the original source response;
- reprocess data without calling the external API again;
- debug transformation errors;
- maintain traceability between source data and transformed data;
- evolve Silver logic independently from ingestion.

---

## 🔗 ADF → Databricks Integration

After the Bronze file is written successfully, ADF triggers a Databricks Job through the Databricks REST API.

The activity:

```text
Run_Databricks_Job
```

calls:

```text
POST /api/2.2/jobs/run-now
```

Authentication uses a Databricks Personal Access Token supplied at runtime through:

```text
databricks_pat
```

The authorization header is generated dynamically:

```text
Bearer <databricks_pat>
```

### Job parameters passed by ADF

ADF sends the following values to Databricks:

| Parameter | Purpose |
|---|---|
| `file_name` | Bronze file generated in the current pipeline execution |
| `sas_token` | Temporary authorization for reading ADLS |
| `environment` | `dev`, `test`, or `prod` |

The target Databricks Job ID is not hardcoded in the pipeline logic. It is retrieved from the ADF global parameter:

```text
databricks_job_id
```

Likewise, the environment is controlled through:

```text
environment_name
```

This allows the same pipeline definition to be deployed across multiple environments.

---

## ⏳ Databricks Run Monitoring

ADF does not simply submit the Databricks Job and finish.

The pipeline waits for the Databricks execution to reach a terminal state.

Inside the `Until` activity:

```text
Wait_For_Databricks
```

ADF performs the following loop:

```text
Wait 10 seconds
      │
      ▼
GET Databricks run status
      │
      ▼
Store lifecycle state
      │
      ▼
Store result state
      │
      └──── repeat until terminal state
```

The run status is retrieved using:

```text
GET /api/2.2/jobs/runs/get?run_id=<RUN_ID>
```

The pipeline tracks:

```text
life_cycle_state
result_state
```

The loop stops when the Databricks run reaches a terminal lifecycle state such as:

```text
TERMINATED
INTERNAL_ERROR
SKIPPED
```

Finally, `Check_Databricks_Result` verifies whether:

```text
result_state == SUCCESS
```

If not, ADF explicitly fails the pipeline through:

```text
Fail_Databricks_Job
```

This makes Databricks execution part of the ADF pipeline outcome rather than a detached asynchronous task.

---

## 🔥 Databricks Bronze → Silver Transformation

The transformation notebook is versioned in the repository:

```text
databricks/
└── notebooks/
    └── 01_bronze_to_silver.py
```

### Runtime parameters

The notebook receives its configuration through Databricks widgets:

```python
dbutils.widgets.text("file_name", "")
dbutils.widgets.text("sas_token", "")
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("storage_account", "")
```

The values are then retrieved:

```python
file_name = dbutils.widgets.get("file_name")
sas_token = dbutils.widgets.get("sas_token").lstrip("?")
environment = dbutils.widgets.get("environment").lower()
storage_account = dbutils.widgets.get("storage_account")
```

### Environment validation

Only the expected deployment environments are accepted:

```python
allowed_environments = ["dev", "test", "prod"]

if environment not in allowed_environments:
    raise ValueError(...)
```

This prevents accidental writes to an invalid catalog.

---

## 🌐 Reading Bronze Data

Because the project uses Databricks Free Edition, the Azure storage layer and Databricks runtime are not integrated through a native Azure Databricks workspace.

The notebook therefore builds an HTTPS URL dynamically:

```python
url = (
    f"https://{storage_account}.blob.core.windows.net/"
    f"data/bronze/weather/{file_name}"
    f"?{sas_token}"
)
```

The raw JSON is downloaded using:

```python
import requests

response = requests.get(url, timeout=30)
response.raise_for_status()

payload = response.json()
```

The SAS token is provided only at runtime and is never committed to Git.

---

## 🔧 PySpark Transformation

WeatherAPI returns the hourly forecast as a nested JSON structure.

The notebook extracts the hourly observations:

```python
hours = payload["forecast"]["forecastday"][0]["hour"]
```

Each hourly record is normalized into a simpler structure:

```python
rows = [
    {
        "timestamp": h["time"],
        "temperature_c": h["temp_c"],
        "humidity": h["humidity"],
        "wind_kph": h["wind_kph"],
        "condition": h["condition"]["text"]
    }
    for h in hours
]
```

A Spark DataFrame is then created:

```python
df = spark.createDataFrame(rows)
```

The timestamp is converted to a proper Spark timestamp type and an ingestion timestamp is added:

```python
from pyspark.sql import functions as F

silver_df = (
    df
    .withColumn("timestamp", F.to_timestamp("timestamp"))
    .withColumn("ingestion_timestamp", F.current_timestamp())
)
```

---

## 🥈 Silver Delta Layer

The target table is dynamically selected according to the environment:

```python
catalog_name = f"weather_{environment}"
schema_name = "silver"
table_name = "weather"

full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
```

Examples:

```text
weather_dev.silver.weather
weather_test.silver.weather
weather_prod.silver.weather
```

The transformed data is stored in Delta format:

```python
silver_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(full_table_name)
```

The Silver table contains normalized hourly weather observations such as:

| Column | Description |
|---|---|
| `timestamp` | Forecast observation timestamp |
| `temperature_c` | Temperature in Celsius |
| `humidity` | Humidity percentage |
| `wind_kph` | Wind speed |
| `condition` | Weather condition |
| `ingestion_timestamp` | Pipeline ingestion timestamp |

---

## 🧱 Repository Structure

```text
azure-weather-lakehouse/
│
├── .github/
│   └── workflows/
│       ├── azure-oidc-test.yml
│       ├── deploy-adf-test.yml
│       ├── deploy-adf-prod.yml
│       └── deploy-databricks.yml
│
├── adf/
│   ├── dataset/
│   ├── factory/
│   ├── linkedService/
│   ├── pipeline/
│   └── publish_config.json
│
├── databricks/
│   ├── databricks.yml
│   ├── notebooks/
│   │   └── 01_bronze_to_silver.py
│   └── resources/
│       └── job_weather_dev.job.yml
│
├── .gitignore
└── README.md
```

ADF-generated ARM templates are stored separately in the:

```text
adf_publish
```

branch.

---

# 🚀 CI/CD — Azure Data Factory

Azure Data Factory uses two Git branches with different responsibilities.

```text
main
  │
  └── ADF source definitions

adf_publish
  │
  └── generated ARM deployment templates
```

ADF is connected to GitHub only in the DEV environment.

When DEV is published, Azure Data Factory generates ARM templates in:

```text
adf_publish
```

GitHub Actions then deploys those templates to TEST and PROD.

### Deployment flow

```text
ADF DEV
   │
   ▼
Publish
   │
   ▼
adf_publish
   │
   ├────────► Deploy TEST
   │
   └────────► Deploy PROD
```

Environment-specific values such as:

```text
ADF factory name
ADLS URL
Service Principal Client ID
Databricks Job ID
environment name
```

are supplied through GitHub Environment Variables instead of being hardcoded.

---

## 🔐 Azure OIDC Authentication

GitHub Actions authenticates to Azure using **OpenID Connect (OIDC)**.

A dedicated Azure App Registration is used for CI/CD:

```text
sp-weather-lakehouse-cicd
```

Federated credentials are configured for GitHub environments such as:

```text
test
prod
```

This avoids storing an Azure client secret in GitHub.

The CI/CD service principal has scoped Contributor permissions only on the required TEST and PROD resource groups.

---

# 🧩 Databricks Declarative Automation Bundles

Databricks Jobs are managed as code using **Declarative Automation Bundles**.

The bundle configuration is stored in:

```text
databricks/databricks.yml
```

The Job resource is stored in:

```text
databricks/resources/
```

The bundle defines three deployment targets:

```text
dev
test
prod
```

Each target supplies its own environment-specific values.

Conceptually:

```text
                  Same notebook
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
        DEV           TEST         PROD
          │            │            │
   DEV storage   TEST storage   PROD storage
          │            │            │
   weather_dev   weather_test   weather_prod
```

### Existing Job binding

The Databricks Jobs were originally created manually.

They were then linked to the bundle using:

```bash
databricks bundle deployment bind
```

Binding tells Databricks:

```text
"Do not create a new Job.
This bundle resource represents this existing Job."
```

This allowed the existing DEV / TEST / PROD jobs to be brought under Infrastructure-as-Code management without recreating them.

---

## Databricks Bundle Workflow

Before deployment, the bundle can be validated:

```bash
databricks bundle validate -t dev
```

A deployment preview can be generated with:

```bash
databricks bundle plan -t dev
```

The configuration is deployed with:

```bash
databricks bundle deploy -t dev
```

The same operations are available for:

```text
test
prod
```

---

# 🔄 CI/CD — Databricks

Databricks deployment is automated through GitHub Actions.

The deployment order is:

```text
DEV
 │
 ▼
TEST
 │
 ▼
PROD
```

For each target, GitHub performs:

```text
Checkout repository
        │
        ▼
Install Databricks CLI
        │
        ▼
bundle validate
        │
        ▼
bundle deploy
```

The Databricks workspace host is stored as a GitHub variable:

```text
DATABRICKS_HOST
```

The CI/CD token is stored as a GitHub secret:

```text
DATABRICKS_TOKEN
```

The token used by GitHub CI/CD is intentionally separate from the Databricks token used by ADF at pipeline runtime.

---

# 🔐 Security

No runtime credentials are committed to Git.

Sensitive values include:

```text
WeatherAPI API key
Databricks PAT
ADLS SAS token
Azure Service Principal Client Secret
Databricks CI/CD token
```

They are supplied through:

- ADF runtime parameters;
- GitHub Secrets;
- Azure / GitHub OIDC;
- short-lived SAS tokens.

The repository includes `.gitignore` rules for local secret files and security scan output:

```gitignore
gitleaks*.json
.env
.env.*
!.env.example
.databrickscfg
```

Before publication, the Git history was also scanned using **Gitleaks**.

---

# ▶️ Running the Pipeline

The pipeline can currently be triggered manually from Azure Data Factory.

Required runtime parameters:

```text
weather_api_key
databricks_pat
sas_token
```

Example conceptual execution:

```text
ADF DEV
  │
  ├── WeatherAPI Key
  ├── Databricks Runtime PAT
  └── ADLS SAS Token
          │
          ▼
WeatherAPI
          │
          ▼
ADLS Bronze
          │
          ▼
Databricks Job
          │
          ▼
weather_dev.silver.weather
```

The Databricks schedules are intentionally paused because the current storage-access implementation uses a temporary SAS token supplied by ADF.

---

# 🔎 Validation and Observability

The project includes several layers of execution validation.

### ADF

ADF monitors the Databricks run lifecycle through the Databricks Jobs REST API.

The pipeline fails if the Databricks execution does not complete successfully.

### Databricks

The notebook validates the requested environment before writing data.

The destination catalog is selected dynamically according to the deployment target.

### CI/CD

Before Databricks deployment:

```text
bundle validate
```

checks the bundle configuration.

Before applying major changes locally:

```text
bundle plan
```

can be used to verify whether resources will be created, updated, or deleted.

### Security

The Git history is scanned with:

```bash
gitleaks git . --redact
```

to detect accidentally committed credentials.

---

# 🛠️ Tech Stack

| Technology | Usage |
|---|---|
| Azure Data Factory | Pipeline orchestration and REST ingestion |
| Azure Data Lake Storage Gen2 | Bronze/raw storage |
| WeatherAPI | External weather data source |
| Databricks | Transformation and workflow execution |
| PySpark | Data transformation |
| Delta Lake | Silver storage layer |
| Databricks Declarative Automation Bundles | Databricks Jobs as code |
| ARM Templates | ADF deployment across environments |
| GitHub Actions | CI/CD automation |
| Azure OIDC | Secretless GitHub → Azure authentication |
| Git / GitHub | Version control |
| Gitleaks | Repository secret scanning |

---

# ⚠️ Current Limitations

The project intentionally keeps the implementation lightweight.

Current limitations include:

- Databricks Free Edition is used instead of a dedicated Azure Databricks workspace;
- access from Databricks to ADLS currently uses HTTPS + SAS;
- SAS tokens are currently supplied manually at pipeline runtime;
- Databricks job schedules are therefore paused;
- only the Bronze and Silver layers are currently implemented;
- automated data-quality tests are not yet implemented;
- TEST and PROD are logically separated inside the same Databricks workspace.

These limitations are known architectural trade-offs rather than hidden assumptions.

---

# 🚀 Future Improvements

Possible improvements include:

- replace temporary SAS authentication with a production-grade identity-based access pattern;
- integrate Azure Key Vault for runtime secrets;
- add an automated daily ADF trigger;
- implement a Gold aggregation layer;
- introduce automated PySpark data-quality tests;
- add schema validation and expectations;
- build weather analytics or BI dashboards on top of the Gold layer;
- use separate Databricks workspaces for DEV / TEST / PROD;
- migrate to a fully Azure-native Databricks architecture;
- add deployment approvals before PROD;
- add monitoring and alerting for pipeline failures;
- add data lineage and governance controls.

---

# 📌 Project Outcomes

This project demonstrates practical knowledge of:

- end-to-end cloud data-pipeline design;
- REST API ingestion;
- raw-data preservation;
- Azure Data Factory orchestration;
- ADLS Gen2;
- PySpark transformations;
- Delta Lake;
- Medallion Architecture concepts;
- asynchronous job orchestration and polling;
- parameterized multi-environment deployments;
- DEV / TEST / PROD separation;
- Infrastructure as Code;
- ARM template deployment;
- Databricks Declarative Automation Bundles;
- GitHub Actions CI/CD;
- Azure OIDC authentication;
- secret management;
- Git-based development workflows;
- deployment validation;
- security scanning with Gitleaks.

The final solution provides a reproducible weather-data pipeline in which the same source code can be promoted across DEV, TEST, and PROD while environment-specific configuration remains externalized.

---

# 👨‍💻 Author

**Raffaele Marro**  
Data Engineer | Databricks | PySpark | Azure Data Factory | Azure | Data Engineering

[LinkedIn](https://www.linkedin.com/in/raffaele-marro-6b1681282/)
