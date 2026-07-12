# Sales ETL — CI/CD on Databricks Free Edition

A minimal, real, working CI/CD pipeline you can stand up in an evening on
**Databricks Free Edition**. GitHub push → tests run → bundle deploys →
Bronze/Silver/Gold job runs automatically.

## Project layout

```
sales-etl-cicd/
├── databricks.yml              # bundle definition (dev/prod targets)
├── resources/
│   └── sales_job.yml           # the job: bronze -> silver -> gold
├── src/
│   ├── bronze_ingestion.py
│   ├── silver_transformation.py
│   └── gold_sales_summary.py
├── tests/
│   └── test_silver_transformation.py   # pytest, runs with local Spark
├── data/
│   └── sales_raw_sample.csv    # synthetic Swiggy/Flipkart/Zerodha data
├── requirements.txt
└── .github/workflows/deploy.yml
```

## What's different because it's Free Edition

Free Edition is a single workspace, one user, **serverless-only** compute,
and it has **no admin console / service principals**. So this project:
- Uses a **personal access token (PAT)**, not a service principal, for CI auth.
- Has no cluster config anywhere — every task runs on serverless compute.
- Treats "prod" as a different Unity Catalog **schema** (`sales_prod`) in the
  same workspace, not a separate workspace. (In a paid account, swap the
  `prod` target's host to a real separate workspace URL.)

## Step-by-step setup

### 1. Sign up and get a workspace
Go to https://www.databricks.com/product/free-edition and sign up. You get one
serverless workspace, Unity Catalog, notebooks, jobs, and SQL — free.

### 2. Create a personal access token
In the workspace: **Settings → Developer → Access tokens → Generate new token**.
Copy it immediately, it's only shown once.

### 3. Upload the sample data to a Unity Catalog volume
In the workspace UI: **Catalog → workspace catalog → create schema `sales_dev`
→ create a Volume named `raw_files`**, then upload `data/sales_raw_sample.csv`
into it. This gives you the path referenced in `databricks.yml`
(`/Volumes/workspace/sales_dev/raw_files/sales_raw_sample.csv`).

### 4. Create a GitHub repo and push this project
```bash
cd sales-etl-cicd
git init
git add .
git commit -m "Initial CI/CD sales ETL project"
git branch -M main
git remote add origin https://github.com/<you>/sales-etl-cicd.git
git push -u origin main
```

### 5. Install the Databricks CLI locally and configure auth
```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
databricks configure --token
# Host:  https://<your-workspace-url>
# Token: <the PAT from step 2>
```

### 6. Validate and deploy locally first (sanity check before automating)
```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run sales_etl_job -t dev
```
Check the workspace: **Catalog → workspace → sales_dev** should now show
`bronze_sales`, `silver_sales`, and `gold_sales_summary` tables.

### 7. Add GitHub Secrets (this is what makes it CI/CD, not just local CLI use)
In your GitHub repo: **Settings → Secrets and variables → Actions → New
repository secret**
- `DATABRICKS_HOST` = `https://<your-workspace-url>`
- `DATABRICKS_TOKEN` = the same PAT from step 2

### 8. Push a change and watch the pipeline run
Edit `src/silver_transformation.py` — e.g. change how missing quantity is
filled — then:
```bash
git checkout -b fix/quantity-default
git commit -am "Adjust missing-quantity default"
git push origin fix/quantity-default
```
Open a Pull Request on GitHub. The **test-and-validate** job runs
automatically: pytest + `bundle validate`. Merge to `main`, and the
**deploy-and-run** job deploys the bundle to the `sales_prod` schema and
triggers the job — no manual clicks.

### 9. Verify
**Workflows** tab in the Databricks workspace UI will show a run of
`sales-etl-prod`. Query `workspace.sales_prod.gold_sales_summary` in a
notebook or SQL editor to see the result.

## Extending this for your CoE students
- Swap `data/sales_raw_sample.csv` for a bigger synthetic dataset.
- Add a 4th task that runs a Great Expectations / simple assertion-based
  data-quality check between Silver and Gold, and fail the job on violation.
- Add a second job resource for a different domain (e.g. a Zerodha trades
  pipeline) to show multiple resources in one bundle.
