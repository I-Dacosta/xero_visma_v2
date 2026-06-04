# Sync This Workspace To Google Cloud Dataform

This repository contains two helper scripts for syncing the local Dataform workspace with a Google Cloud Dataform workspace:

- `sync_to_gcp.py` uploads local files to a remote Dataform workspace, then commits and pushes the workspace.
- `sync_from_gcp.py` downloads files from a remote Dataform workspace into this local folder.

Note: these scripts target **Dataform**, not Dataflow. If you meant Google Cloud Dataflow, that is a different service and is not what these scripts use.

## What Gets Synced

The sync scripts work from the repository root and include the Dataform project files, such as:

- `workflow_settings.yaml`
- `definitions/`
- supporting project files in the root folder

The upload script skips these folders and files:

- `.git`
- `my_venv`
- `node_modules`
- `__pycache__`
- `.DS_Store`
- `.df-credentials.json`
- `.df-credentials.json.bak_invalid_schema`

## Prerequisites

Before syncing, make sure you have:

1. Python available locally.
2. The Google Cloud SDK installed.
3. Access to the target Google Cloud Dataform repository and workspace.
4. Logged in with `gcloud`.

Example:

```bash
gcloud auth login
gcloud config set project prj-dw-dev
gcloud auth print-access-token
```

If `gcloud auth print-access-token` returns a token, the sync scripts can authenticate.

## Activate The Local Environment

If you want to use the existing virtual environment in this repo:

```bash
source my_venv/bin/activate
```

The sync scripts themselves only require Python and `gcloud`, but using the repo virtual environment keeps your workflow consistent.

## Configure The Target Dataform Workspace

Both sync scripts use hardcoded constants at the top of the file. Check these before running them.

In `sync_to_gcp.py`:

```python
PROJECT = "prj-dw-dev"
LOCATION = "europe-north1"
REPOSITORY = "Datawarehouse"
WORKSPACE = "Dev"
```

In `sync_from_gcp.py`:

```python
PROJECT = "prj-dw-dev"
LOCATION = "europe-north1"
REPOSITORY = "kim-test-repo"
WORKSPACE = "kim-rormark"
```

Important:

- `sync_to_gcp.py` and `sync_from_gcp.py` are currently configured for different repositories/workspaces.
- Update the constants in each script so they point to the workspace you actually want to use.
- `workflow_settings.yaml` contains the Dataform project defaults for compilation and execution, but the sync scripts use their own API target settings.

## Sync Local Changes To GCP

Run this from the repository root:

```bash
python3 sync_to_gcp.py
```

Or provide a custom commit message:

```bash
python3 sync_to_gcp.py "Sync local Dataform workspace files"
```

What this does:

1. Reads all local files in this folder.
2. Compares each file to the remote Dataform workspace.
3. Uploads only files that changed.
4. Creates a commit in the remote Dataform workspace.
5. Pushes the commit to the connected Git remote.

Typical output looks like this:

```text
Syncing /path/to/Dataform to prj-dw-dev/europe-north1/Datawarehouse/Dev
Uploaded  definitions/silver/visma/fact_customer_payment.sqlx
Uploaded  workflow_settings.yaml

Uploaded : 2
Unchanged: 100
Committed: True
Pushed   : True
```

## Sync Remote Changes Down To Local

Run this from the repository root:

```bash
python3 sync_from_gcp.py
```

What this does:

1. Lists files in the configured remote Dataform workspace.
2. Downloads each file.
3. Writes changed files into this local folder.
4. Leaves unchanged files alone.

Typical output looks like this:

```text
Listing workspace: Dev
Found 102 files

Updated : 3
Unchanged: 99
Failed   : 0
```

## Recommended Workflow

Use this sequence when working on the project:

1. Pull the latest remote workspace if someone else may have changed it.
2. Make local edits.
3. Validate locally.
4. Push local changes back to the Dataform workspace.

Example:

```bash
source my_venv/bin/activate
python3 sync_from_gcp.py
dataform compile
python3 sync_to_gcp.py "Update Visma silver models"
```

## Validate Before Syncing

Before pushing changes, it is safer to compile locally:

```bash
dataform compile
```

If your workflow also includes custom upload or verification steps, run those before syncing.

## Troubleshooting

### `gcloud auth print-access-token` fails

Run:

```bash
gcloud auth login
gcloud config set project prj-dw-dev
```

### HTTP 404 or HTTP 403 from the sync script

Check:

- `PROJECT`
- `LOCATION`
- `REPOSITORY`
- `WORKSPACE`
- your access to the Dataform repository

### Files sync to the wrong workspace

The scripts use hardcoded configuration. Open the script you are running and verify the constants before retrying.

### `sync_to_gcp.py` pushes unexpected files

The script syncs nearly everything in the repository root except the explicit skip list. If you add local-only files that should never be uploaded, extend the `SKIP_DIRS` or `SKIP_FILES` sets in `sync_to_gcp.py`.

## Relevant Files

- `sync_to_gcp.py`
- `sync_from_gcp.py`
- `workflow_settings.yaml`
- `definitions/`

## Terminal-First GCP Workflow

This repo can now be worked entirely from a terminal without using the Google Cloud Console UI.

### Environment In This Repo

- Google Cloud project: `prj-dw-dev`
- Dataform repository: `Datawarehouse`
- Dataform workspace: `Dev`
- Dataform API location: `europe-north1`
- BigQuery execution location from `workflow_settings.yaml`: `europe-north2`

That last point matters:

- Dataform repository and workspace are managed in `europe-north1`
- BigQuery datasets for this project are executing in `europe-north2`
- the helper scripts below keep those concerns separate on purpose
- `workflow_settings.yaml` currently points at `defaultDataset: dw_1_silver_visma_global`, which is not present in BigQuery from this account, so models that rely on the default dataset rather than explicit schema config may fail

### Added Scripts

- `scripts/auth-gcp.sh`
- `scripts/check-gcp-access.sh`
- `scripts/bq-query.sh`
- `scripts/dataform-run.sh`
- `scripts/dataform-workflow-invoke.sh`
- `scripts/watch-dataform-workflow.sh`
- `scripts/run-dataflow-job.sh`
- `scripts/watch-dataflow-job.sh`
- `scripts/tail-dataflow-logs.sh`

### One-Time Setup

Authenticate and pin `gcloud` to this project and region:

```bash
./scripts/auth-gcp.sh
```

What it does:

1. Ensures `gcloud` is authenticated.
2. Sets the active project to `prj-dw-dev`.
3. Sets the default compute region to `europe-north1`.

### Verify Access

Run the full terminal smoke test:

```bash
./scripts/check-gcp-access.sh
```

That verifies:

- `gcloud` auth and active config
- BigQuery dataset listing
- a live BigQuery query
- Dataform repository access
- Dataform workspace listing
- Dataflow job listing
- local `dataform compile`

### Local SQLX Workflow

Compile locally:

```bash
dataform compile
```

Dry-run a single model against BigQuery without applying changes:

```bash
./scripts/dataform-run.sh --dry-run --actions fact_sales_order
```

Run a model for real:

```bash
./scripts/dataform-run.sh --actions fact_sales_order
```

When an action name is ambiguous, use the full `project.dataset.action` form:

```bash
./scripts/dataform-run.sh \
  --dry-run \
  --actions prj-dw-dev.dw_1_silver_visma.fact_sales_order_line
```

The terminal validations already confirmed:

- `fact_sales_order` dry-run works
- `prj-dw-dev.dw_1_silver_visma.fact_sales_order_line` dry-run works

### Managed Dataform Workflow

Invoke the Google Cloud Dataform workspace directly from the terminal:

```bash
./scripts/dataform-workflow-invoke.sh \
  --target fact_sales_order \
  --target prj-dw-dev.dw_1_silver_visma.fact_sales_order_line \
  --include-deps
```

Watch that remote invocation:

```bash
./scripts/watch-dataform-workflow.sh <workflow-invocation-id-or-full-name>
```

Use this when you want to run the managed `Dev` workspace instead of the local CLI runner.

### BigQuery Queries

Run ad hoc queries in the correct project and location:

```bash
./scripts/bq-query.sh 'select current_date() as today, 1 as ok'
```

Example for validating a changed model:

```bash
./scripts/bq-query.sh '
select
  tenant_id,
  business_unit_id,
  business_unit_name,
  order_id
from `prj-dw-dev.dw_1_silver_visma.fact_sales_order`
where tenant_id = "33f319b9-4d57-11ee-960c-025417856183"
limit 20
'
```

### Dataflow Workflow

Run a classic template job:

```bash
./scripts/run-dataflow-job.sh \
  --job-name my-classic-job-$(date +%Y%m%d-%H%M%S) \
  --classic-template gs://MY_BUCKET/templates/MY_TEMPLATE \
  --parameters input=gs://MY_BUCKET/in,output=gs://MY_BUCKET/out
```

Run a flex template job:

```bash
./scripts/run-dataflow-job.sh \
  --job-name my-flex-job-$(date +%Y%m%d-%H%M%S) \
  --flex-template gs://MY_BUCKET/templates/MY_TEMPLATE.json \
  --parameters input=gs://MY_BUCKET/in,output=gs://MY_BUCKET/out \
  --temp-location=gs://MY_BUCKET/tmp
```

Watch job status:

```bash
./scripts/watch-dataflow-job.sh <job-id>
```

Tail logs live:

```bash
./scripts/tail-dataflow-logs.sh <job-id>
```

The log tail uses `gcloud beta logging tail`. The beta component is installed locally in this environment now.

### Quick Start

Start a SQLX check:

```bash
./scripts/dataform-run.sh --dry-run --actions fact_sales_order
```

Start a managed Dataform run:

```bash
./scripts/dataform-workflow-invoke.sh --target fact_sales_order --include-deps
```

Start a Dataflow job:

```bash
./scripts/run-dataflow-job.sh --job-name my-job --flex-template gs://... --parameters ...
```

Monitor it live:

```bash
./scripts/watch-dataflow-job.sh <job-id>
./scripts/tail-dataflow-logs.sh <job-id>
```

Debug failures:

1. Check the terminal state watcher first.
2. Tail logs live with `./scripts/tail-dataflow-logs.sh <job-id>`.
3. Re-run the exact failing SQL in BigQuery with `./scripts/bq-query.sh`.
4. Dry-run the affected SQLX model with `./scripts/dataform-run.sh --dry-run --actions ...`.

### Suggested Shell Aliases

If you want faster iteration in your terminal profile:

```bash
alias dfauth='./scripts/auth-gcp.sh'
alias dfcheck='./scripts/check-gcp-access.sh'
alias dfrun='./scripts/dataform-run.sh'
alias dfwatch='./scripts/watch-dataform-workflow.sh'
alias bqq='./scripts/bq-query.sh'
alias dfjob='./scripts/run-dataflow-job.sh'
alias dfwatchjob='./scripts/watch-dataflow-job.sh'
alias dflogs='./scripts/tail-dataflow-logs.sh'
```
