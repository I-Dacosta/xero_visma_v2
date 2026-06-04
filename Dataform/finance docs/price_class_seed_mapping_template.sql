-- Customer price class seed mapping template
-- Purpose:
--   Produce a business-reviewable CSV template for validating every currently observed
--   customer class candidate and every observed CustomerPriceClass code in bronze.
--
-- Output columns:
--   tenant_id / organization_name
--   source_field / source_code
--   distinct_customer_count + sample_customers   -> signal from visma_customers.customerClassId
--   distinct_item_count + sample_items           -> signal from visma_customer_sales_prices.CustomerPriceClass
--   candidate_lookup_*                           -> optional lookup signal from visma_attributes.SALESRESTR
--   validated_* / mapping_status / notes         -> fields for business validation
--
-- Usage:
--   bq query --use_legacy_sql=false --format=csv < "finance docs/price_class_seed_mapping_template.sql" \
--     > "finance docs/price_class_seed_mapping_template.csv"

WITH salesrestr_lookup AS (
  SELECT
    JSON_VALUE(item, '$.valueId') AS source_code,
    STRING_AGG(
      DISTINCT JSON_VALUE(item, '$.description'),
      ' | '
      ORDER BY JSON_VALUE(item, '$.description')
    ) AS candidate_lookup_name,
    STRING_AGG(DISTINCT tenantId, ' | ' ORDER BY tenantId) AS candidate_lookup_tenant_id,
    STRING_AGG(
      DISTINCT organizationName,
      ' | '
      ORDER BY organizationName
    ) AS candidate_lookup_organization_name,
    'visma_attributes.SALESRESTR' AS candidate_lookup_source
  FROM `prj-dw-dev.dw_1_bronze_visma.visma_attributes`,
    UNNEST(JSON_EXTRACT_ARRAY(details)) AS item
  WHERE isDeleted IS DISTINCT FROM TRUE
    AND attributeID = 'SALESRESTR'
    AND JSON_VALUE(item, '$.valueId') IS NOT NULL
  GROUP BY 1
),

customer_class_review AS (
  SELECT
    tenantId AS tenant_id,
    organizationName AS organization_name,
    'customerClassId' AS source_field,
    customerClassId AS source_code,
    COUNT(DISTINCT number) AS distinct_customer_count,
    ARRAY_TO_STRING(
      ARRAY_AGG(DISTINCT name IGNORE NULLS ORDER BY name LIMIT 10),
      ' | '
    ) AS sample_customers,
    CAST(NULL AS INT64) AS distinct_item_count,
    CAST(NULL AS STRING) AS sample_items
  FROM `prj-dw-dev.dw_1_bronze_visma.visma_customers`
  WHERE isDeleted IS DISTINCT FROM TRUE
    AND customerClassId IS NOT NULL
  GROUP BY 1, 2, 3, 4
),
customer_price_class_review AS (
  SELECT
    tenantId AS tenant_id,
    organizationName AS organization_name,
    'customer_sales_prices.CustomerPriceClass' AS source_field,
    priceCode AS source_code,
    CAST(NULL AS INT64) AS distinct_customer_count,
    CAST(NULL AS STRING) AS sample_customers,
    COUNT(DISTINCT inventoryId) AS distinct_item_count,
    ARRAY_TO_STRING(
      ARRAY_AGG(DISTINCT description IGNORE NULLS ORDER BY description LIMIT 10),
      ' | '
    ) AS sample_items
  FROM `prj-dw-dev.dw_1_bronze_visma.visma_customer_sales_prices`
  WHERE isDeleted IS DISTINCT FROM TRUE
    AND priceType = 'CustomerPriceClass'
    AND priceCode IS NOT NULL
  GROUP BY 1, 2, 3, 4
),
review_rows AS (
  SELECT * FROM customer_class_review
  UNION ALL
  SELECT * FROM customer_price_class_review
)
SELECT
  r.tenant_id,
  r.organization_name,
  r.source_field,
  r.source_code,
  r.distinct_customer_count,
  r.sample_customers,
  r.distinct_item_count,
  r.sample_items,
  COALESCE(s.candidate_lookup_name, '') AS candidate_lookup_name,
  COALESCE(s.candidate_lookup_source, '') AS candidate_lookup_source,
  COALESCE(s.candidate_lookup_tenant_id, '') AS candidate_lookup_tenant_id,
  COALESCE(s.candidate_lookup_organization_name, '') AS candidate_lookup_organization_name,
  '' AS validated_price_class_id,
  '' AS validated_price_class_name,
  '' AS validated_customer_group_name,
  '' AS include_in_final_mapping,
  'pending_business_review' AS mapping_status,
  '' AS notes
FROM review_rows AS r
LEFT JOIN salesrestr_lookup AS s
  ON s.source_code = r.source_code
ORDER BY
  r.tenant_id,
  r.organization_name,
  r.source_field,
  SAFE_CAST(r.source_code AS INT64),
  r.source_code;
