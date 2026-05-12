#!/usr/bin/env bash
# Provision the BigQuery dataset + per-entity tables that `xero-state::bq_sink`
# streams into. Idempotent: re-running is safe (ALREADY_EXISTS treated as ok).
#
# Required: `bq` CLI on PATH with creds that have BigQuery Admin on the project
# OR pre-existing dataset and an SA with Data Editor + Job User on the dataset.
#
# Env vars (with defaults matching .env):
#   GCP_PROJECT_ID   default prj-dw-dev
#   BIGQUERY_DATASET default dw_1_bronze_xero
#   BQ_LOCATION      default europe-north1
set -euo pipefail

PROJECT=${GCP_PROJECT_ID:-prj-dw-dev}
DATASET=${BIGQUERY_DATASET:-dw_1_bronze_xero}
LOCATION=${BQ_LOCATION:-europe-north1}

echo "== ensuring dataset ${PROJECT}:${DATASET} (location=${LOCATION}) =="
if bq --project_id="${PROJECT}" show --dataset "${PROJECT}:${DATASET}" >/dev/null 2>&1; then
    echo "  dataset exists"
else
    bq --project_id="${PROJECT}" mk --dataset --location="${LOCATION}" "${PROJECT}:${DATASET}"
    echo "  dataset created"
fi

# Envelope schema — one row per (tenant_id, record_id) per (logical) upsert.
# Downstream queries do JSON_VALUE(payload, '$.Field') to pivot into typed views.
SCHEMA='[
    {"name":"tenant_id","type":"STRING","mode":"REQUIRED"},
    {"name":"record_id","type":"STRING","mode":"REQUIRED"},
    {"name":"payload","type":"STRING","mode":"REQUIRED"},
    {"name":"first_seen_at","type":"TIMESTAMP","mode":"REQUIRED"},
    {"name":"last_seen_at","type":"TIMESTAMP","mode":"REQUIRED"},
    {"name":"last_run_id","type":"STRING","mode":"REQUIRED"},
    {"name":"synced_at","type":"TIMESTAMP","mode":"REQUIRED"}
]'

# Mirror of `EntityType::all()` (28 entities) — PascalCase table names match
# `BigQueryStreamingSink::table_id`.
ENTITIES=(
    Accounts BankTransactions BankTransfers BatchPayments BrandingThemes
    Budgets ContactGroups Contacts CreditNotes Currencies ExpenseClaims
    Invoices Items Journals LinkedTransactions ManualJournals Organisations
    Overpayments Payments PaymentServices Prepayments PurchaseOrders Quotes
    Receipts RepeatingInvoices TaxRates TrackingCategories Users
)

for TABLE in "${ENTITIES[@]}"; do
    if bq --project_id="${PROJECT}" show "${PROJECT}:${DATASET}.${TABLE}" >/dev/null 2>&1; then
        echo "  table ${TABLE} exists"
    else
        bq --project_id="${PROJECT}" mk --table \
            --time_partitioning_field=last_seen_at \
            --time_partitioning_type=DAY \
            --clustering_fields=tenant_id \
            "${PROJECT}:${DATASET}.${TABLE}" "${SCHEMA}"
        echo "  table ${TABLE} created (day-partitioned on last_seen_at, clustered on tenant_id)"
    fi
done

echo ""
echo "Provision complete. Sink target: ${PROJECT}:${DATASET}"
echo "Test from the service: curl -X POST http://localhost:5002/tenants/\$T/bq/replay -d '{\"limit\":50}'"
