-- Price class verification plan
-- Purpose:
--   Verify which bronze source actually contains the customer -> price class mapping,
--   list every observed code/value, and produce the evidence needed for a correct mapping.
--
-- Relevant bronze sources:
--   1. prj-dw-dev.dw_1_bronze_visma.visma_customers
--      Candidate source for customer membership in a price class.
--   2. prj-dw-dev.dw_1_bronze_visma.visma_customer_classes
--      Candidate lookup/master for customerClassId descriptions.
--      This table is currently declared in Dataform, but may be missing physically in bronze.
--   3. prj-dw-dev.dw_1_bronze_visma.visma_customer_sales_prices
--      Evidence that price class codes exist in the environment via priceCode / priceType.
--      This table does NOT contain customer_id, so it cannot on its own map customer -> price class.
--
-- Decision rules after running this script:
--   A. If visma_customers.priceClass is populated and stable, use that as the customer price class source.
--   B. If business confirms customerClassId is the intended business "price class", then use
--      visma_customers.customerClassId + visma_customer_classes.description.
--   C. If neither A nor B is true, then a separate source or manual mapping table is required.

DECLARE has_customer_classes BOOL DEFAULT EXISTS (
  SELECT 1
  FROM `prj-dw-dev.dw_1_bronze_visma.INFORMATION_SCHEMA.TABLES`
  WHERE table_name = 'visma_customer_classes'
);

-- ============================================================================
-- 1. Raw field coverage in visma_customers
--    This shows whether customers.priceClass actually carries data in bronze.
-- ============================================================================
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(isDeleted IS DISTINCT FROM TRUE) AS active_rows,
  COUNTIF(isDeleted IS DISTINCT FROM TRUE AND priceClass IS NOT NULL) AS priceclass_raw_present,
  COUNTIF(isDeleted IS DISTINCT FROM TRUE AND JSON_VALUE(priceClass, '$.id') IS NOT NULL) AS priceclass_id_present,
  COUNTIF(isDeleted IS DISTINCT FROM TRUE AND JSON_VALUE(priceClass, '$.description') IS NOT NULL) AS priceclass_description_present,
  COUNTIF(isDeleted IS DISTINCT FROM TRUE AND customerClassId IS NOT NULL) AS customerclassid_present,
  COUNTIF(isDeleted IS DISTINCT FROM TRUE AND customerClass IS NOT NULL) AS customerclass_raw_present,
  COUNTIF(isDeleted IS DISTINCT FROM TRUE AND JSON_VALUE(customerClass, '$.id') IS NOT NULL) AS customerclass_json_id_present,
  COUNTIF(isDeleted IS DISTINCT FROM TRUE AND JSON_VALUE(customerClass, '$.description') IS NOT NULL) AS customerclass_json_description_present
FROM `prj-dw-dev.dw_1_bronze_visma.visma_customers`;

-- ============================================================================
-- 2. Distinct raw payloads in visma_customers.priceClass
--    If this returns no rows, then priceClass is not currently usable as a source.
-- ============================================================================
SELECT
  priceClass,
  JSON_VALUE(priceClass, '$.id') AS price_class_id,
  JSON_VALUE(priceClass, '$.description') AS price_class_description,
  COUNT(*) AS row_count
FROM `prj-dw-dev.dw_1_bronze_visma.visma_customers`
WHERE isDeleted IS DISTINCT FROM TRUE
  AND priceClass IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY row_count DESC, price_class_id;

-- ============================================================================
-- 3. All observed customer class values from visma_customers
--    This is the current effective customer grouping in bronze.
-- ============================================================================
SELECT
  tenantId AS tenant_id,
  organizationName AS organization_name,
  customerClassId AS customer_class_id,
  JSON_VALUE(customerClass, '$.id') AS customer_class_json_id,
  JSON_VALUE(customerClass, '$.description') AS customer_class_json_description,
  COUNT(DISTINCT number) AS distinct_customers,
  ARRAY_AGG(DISTINCT name IGNORE NULLS ORDER BY name LIMIT 20) AS sample_customers
FROM `prj-dw-dev.dw_1_bronze_visma.visma_customers`
WHERE isDeleted IS DISTINCT FROM TRUE
GROUP BY 1, 2, 3, 4, 5
ORDER BY tenant_id, customer_class_id;

-- ============================================================================
-- 4. Compact review table for all customerClassId values across tenants
--    Useful when building an external seed mapping if business wants to map these codes.
-- ============================================================================
SELECT
  customerClassId AS customer_class_id,
  COUNT(DISTINCT CONCAT(tenantId, '|', number)) AS distinct_customer_keys,
  COUNT(DISTINCT tenantId) AS tenant_count,
  ARRAY_AGG(DISTINCT organizationName IGNORE NULLS ORDER BY organizationName LIMIT 20) AS sample_organizations,
  ARRAY_AGG(DISTINCT name IGNORE NULLS ORDER BY name LIMIT 30) AS sample_customers
FROM `prj-dw-dev.dw_1_bronze_visma.visma_customers`
WHERE isDeleted IS DISTINCT FROM TRUE
GROUP BY 1
ORDER BY SAFE_CAST(customer_class_id AS INT64), customer_class_id;

-- ============================================================================
-- 5. Check whether visma_customer_classes is physically available in bronze
-- ============================================================================
SELECT has_customer_classes AS has_customer_classes;

-- ============================================================================
-- 6. If visma_customer_classes exists, show the lookup table and customer join.
--    If missing, emit a status row instead of failing.
-- ============================================================================
IF has_customer_classes THEN
  EXECUTE IMMEDIATE '''
    SELECT
      *
    FROM `prj-dw-dev.dw_1_bronze_visma.visma_customer_classes`
    ORDER BY classId
  ''';

  EXECUTE IMMEDIATE '''
    SELECT
      c.tenantId AS tenant_id,
      c.organizationName AS organization_name,
      c.customerClassId AS customer_class_id,
      cc.classId AS class_id_lookup,
      cc.description AS class_description_lookup,
      COUNT(DISTINCT c.number) AS distinct_customers,
      ARRAY_AGG(DISTINCT c.name IGNORE NULLS ORDER BY c.name LIMIT 20) AS sample_customers
    FROM `prj-dw-dev.dw_1_bronze_visma.visma_customers` AS c
    LEFT JOIN `prj-dw-dev.dw_1_bronze_visma.visma_customer_classes` AS cc
      ON c.customerClassId = CAST(cc.classId AS STRING)
    WHERE c.isDeleted IS DISTINCT FROM TRUE
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY tenant_id, customer_class_id
  ''';
ELSE
  SELECT
    'visma_customer_classes is not present in dw_1_bronze_visma. Load this source before using it for descriptions.' AS status;
END IF;

-- ============================================================================
-- 7. Full price code universe from visma_customer_sales_prices
--    This shows every observed price code in the environment and how heavily it is used.
-- ============================================================================
SELECT
  tenantId AS tenant_id,
  organizationName AS organization_name,
  priceType AS price_type,
  priceCode AS price_code,
  COUNT(*) AS row_count,
  COUNT(DISTINCT inventoryId) AS distinct_items,
  MIN(COALESCE(
    SAFE_CAST(effectiveDate AS DATE),
    DATE(SAFE_CAST(effectiveDate AS DATETIME)),
    DATE(SAFE_CAST(effectiveDate AS TIMESTAMP))
  )) AS first_effective_date,
  MAX(COALESCE(
    SAFE_CAST(expirationDate AS DATE),
    DATE(SAFE_CAST(expirationDate AS DATETIME)),
    DATE(SAFE_CAST(expirationDate AS TIMESTAMP))
  )) AS last_expiration_date,
  ARRAY_AGG(DISTINCT description IGNORE NULLS ORDER BY description LIMIT 20) AS sample_item_descriptions
FROM `prj-dw-dev.dw_1_bronze_visma.visma_customer_sales_prices`
WHERE isDeleted IS DISTINCT FROM TRUE
GROUP BY 1, 2, 3, 4
ORDER BY tenant_id, price_type, price_code;

-- ============================================================================
-- 8. Canonical price code review table across all tenants
--    Use this when mapping all price codes to business names.
-- ============================================================================
SELECT
  priceCode AS price_code,
  priceType AS price_type,
  COUNT(*) AS row_count,
  COUNT(DISTINCT tenantId) AS tenant_count,
  COUNT(DISTINCT inventoryId) AS distinct_items,
  ARRAY_AGG(DISTINCT organizationName IGNORE NULLS ORDER BY organizationName LIMIT 20) AS sample_organizations,
  ARRAY_AGG(DISTINCT description IGNORE NULLS ORDER BY description LIMIT 30) AS sample_item_descriptions
FROM `prj-dw-dev.dw_1_bronze_visma.visma_customer_sales_prices`
WHERE isDeleted IS DISTINCT FROM TRUE
GROUP BY 1, 2
ORDER BY price_type, SAFE_CAST(price_code AS INT64), price_code;

-- ============================================================================
-- 9. Compare the two code universes:
--      customerClassId values from customers
--      priceCode values from customer_sales_prices where priceType = CustomerPriceClass
--    This shows whether they overlap cleanly or represent different concepts.
-- ============================================================================
WITH customer_class_codes AS (
  SELECT DISTINCT
    customerClassId AS code
  FROM `prj-dw-dev.dw_1_bronze_visma.visma_customers`
  WHERE isDeleted IS DISTINCT FROM TRUE
    AND customerClassId IS NOT NULL
),
customer_price_codes AS (
  SELECT DISTINCT
    priceCode AS code
  FROM `prj-dw-dev.dw_1_bronze_visma.visma_customer_sales_prices`
  WHERE isDeleted IS DISTINCT FROM TRUE
    AND priceType = 'CustomerPriceClass'
    AND priceCode IS NOT NULL
)
SELECT
  COALESCE(cc.code, pc.code) AS code,
  cc.code IS NOT NULL AS appears_in_customerClassId,
  pc.code IS NOT NULL AS appears_in_customer_sales_prices
FROM customer_class_codes AS cc
FULL OUTER JOIN customer_price_codes AS pc
  ON cc.code = pc.code
ORDER BY SAFE_CAST(code AS INT64), code;

-- ============================================================================
-- 10. Candidate review output for a future manual/seed mapping table
--     This gives one consolidated review set for business validation.
-- ============================================================================
WITH customer_class_review AS (
  SELECT
    tenantId AS tenant_id,
    organizationName AS organization_name,
    customerClassId AS source_code,
    'customerClassId' AS source_field,
    COUNT(DISTINCT number) AS distinct_customer_count,
    ARRAY_AGG(DISTINCT name IGNORE NULLS ORDER BY name LIMIT 20) AS sample_customers,
    CAST(NULL AS INT64) AS distinct_item_count,
    CAST(NULL AS ARRAY<STRING>) AS sample_items
  FROM `prj-dw-dev.dw_1_bronze_visma.visma_customers`
  WHERE isDeleted IS DISTINCT FROM TRUE
    AND customerClassId IS NOT NULL
  GROUP BY 1, 2, 3, 4
),
customer_sales_price_review AS (
  SELECT
    tenantId AS tenant_id,
    organizationName AS organization_name,
    priceCode AS source_code,
    CONCAT('customer_sales_prices.', priceType) AS source_field,
    CAST(NULL AS INT64) AS distinct_customer_count,
    CAST(NULL AS ARRAY<STRING>) AS sample_customers,
    COUNT(DISTINCT inventoryId) AS distinct_item_count,
    ARRAY_AGG(DISTINCT description IGNORE NULLS ORDER BY description LIMIT 20) AS sample_items
  FROM `prj-dw-dev.dw_1_bronze_visma.visma_customer_sales_prices`
  WHERE isDeleted IS DISTINCT FROM TRUE
    AND priceCode IS NOT NULL
  GROUP BY 1, 2, 3, 4
)
SELECT *
FROM customer_class_review
UNION ALL
SELECT *
FROM customer_sales_price_review
ORDER BY tenant_id, source_field, SAFE_CAST(source_code AS INT64), source_code;

-- ============================================================================
-- 11. Optional named-customer debug query
--     Change the search term when you want a focused drilldown.
-- ============================================================================
SELECT
  tenantId AS tenant_id,
  organizationName AS organization_name,
  number AS customer_id,
  name AS customer_name,
  customerClassId,
  customerClass,
  priceClass
FROM `prj-dw-dev.dw_1_bronze_visma.visma_customers`
WHERE isDeleted IS DISTINCT FROM TRUE
  AND LOWER(name) LIKE '%mowi%'
ORDER BY customer_name;
