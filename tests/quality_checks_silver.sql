/*
===============================================================================
Data Quality Validation – Silver Layer
===============================================================================
Purpose:
    This script performs a comprehensive set of data quality validations
    on tables within the 'silver' schema. It checks for:
      - Null or duplicate values in primary keys
      - Untrimmed (space-padded) string values
      - Invalid or inconsistent date values
      - Improper numerical values (e.g., negative costs or quantities)
      - Data standardization and conformity across fields
      - Logical consistency between related data points

Usage Guidelines:
    - Execute this script after completing the Silver Layer data load.
    - Review and investigate any issues returned by these checks.
    - Any failing records should be addressed upstream or corrected here
      depending on data governance rules.
===============================================================================
*/

-- ====================================================================
-- Validate 'silver.crm_cust_info'
-- ====================================================================

-- Check for nulls or duplicate customer IDs (Primary Key Violation)
-- Expected: No rows returned
SELECT 
    cst_id,
    COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Identify string fields with leading/trailing spaces
-- Expected: No rows returned
SELECT 
    cst_key 
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Check for marital status consistency
SELECT DISTINCT 
    cst_marital_status 
FROM silver.crm_cust_info;

-- ====================================================================
-- Validate 'silver.crm_prd_info'
-- ====================================================================

-- Check for nulls or duplicate product IDs (Primary Key Violation)
-- Expected: No rows returned
SELECT 
    prd_id,
    COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Identify product names with unwanted spaces
-- Expected: No rows returned
SELECT 
    prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Detect negative or null product costs
-- Expected: No rows returned
SELECT 
    prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Review standardization of product line values
SELECT DISTINCT 
    prd_line 
FROM silver.crm_prd_info;

-- Validate logical ordering of product dates (start should precede end)
-- Expected: No rows returned
SELECT 
    * 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- ====================================================================
-- Validate 'silver.crm_sales_details'
-- ====================================================================

-- Check for invalid due dates in raw (bronze) data
-- Expected: No invalid or ill-formatted dates
SELECT 
    NULLIF(sls_due_dt, 0) AS sls_due_dt 
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
   OR LEN(sls_due_dt) != 8 
   OR sls_due_dt > 20500101 
   OR sls_due_dt < 19000101;

-- Ensure order dates are before shipping and due dates
-- Expected: No rows returned
SELECT 
    * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- Validate that sales amount equals quantity multiplied by price
-- Expected: No rows returned
SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- ====================================================================
-- Validate 'silver.erp_cust_az12'
-- ====================================================================

-- Detect birthdates outside of valid historical range
-- Expected: Birthdates between 1924-01-01 and current date
SELECT DISTINCT 
    bdate 
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > GETDATE();

-- Review gender value standardization
SELECT DISTINCT 
    gen 
FROM silver.erp_cust_az12;

-- ====================================================================
-- Validate 'silver.erp_loc_a101'
-- ====================================================================

-- Review country value consistency
SELECT DISTINCT 
    cntry 
FROM silver.erp_loc_a101
ORDER BY cntry;

-- ====================================================================
-- Validate 'silver.erp_px_cat_g1v2'
-- ====================================================================

-- Identify untrimmed values in category-related columns
-- Expected: No rows returned
SELECT 
    * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Review distinct maintenance values for consistency
SELECT DISTINCT 
    maintenance 
FROM silver.erp_px_cat_g1v2;
