-- Create sensitive data table for testing masking policies

USE ROLE TPCH_ADMIN;


-- ============================================================================
-- STEP 1: Create a table with sensitive data (simulated)
-- ============================================================================

CREATE OR REPLACE TABLE ANALYTICS.CUSTOMER_SENSITIVE AS
SELECT 
    C_CUSTKEY,
    C_NAME,
    C_ADDRESS,
    C_PHONE,
    C_ACCTBAL,
    C_NATION_NAME,
    C_REGION_NAME,
    C_MKTSEGMENT,
    -- Add sensitive info (simulation)
    'customer_' || C_CUSTKEY || '@company.com' AS EMAIL,
    LPAD(ABS(MOD(C_CUSTKEY * 123456789, 1000000000)), 9, '0') AS SSN_LAST_9, -- Social Security Number
    UNIFORM(100000000::INT, 999999999::INT, RANDOM()) AS PHONE_NUMBER
    
FROM TPCH_ANALYTICS_DB.ANALYTICS.CUSTOMER_SILVER;

-- ============================================================================
-- STEP 2: Create Masking Policies for Sensitive Columns
-- ============================================================================

-- Masking Policy for Email (show full to ADMIN, partial to ANALYST, mask for SUPPORT)
CREATE OR REPLACE MASKING POLICY CONTROL.MASK_EMAIL AS (val STRING) RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'TPCH_ADMIN', 'TPCH_DEVELOPER') THEN val
        WHEN CURRENT_ROLE() IN ('TPCH_ANALYST') THEN 
            REGEXP_REPLACE(val, '(^[^@]{2})[^@]+', '\\1***')  -- Show first 2 chars before @
        ELSE '***@masked.com'
    END;

-- Masking Policy for Phone (show full to ADMIN, last 4 digits to ANALYST, mask for SUPPORT)
CREATE OR REPLACE MASKING POLICY CONTROL.MASK_PHONE AS (val STRING) RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'TPCH_ADMIN', 'TPCH_DEVELOPER') THEN val
        WHEN CURRENT_ROLE() IN ('TPCH_ANALYST') THEN 
            'XXX-XXX-' || RIGHT(val, 4)
        ELSE 'XXX-XXX-XXX'
    END;

-- Masking Policy for SSN (full mask except for ADMIN)
CREATE OR REPLACE MASKING POLICY CONTROL.MASK_SSN AS (val STRING) RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'TPCH_ADMIN', 'TPCH_DEVELOPER') THEN val
        ELSE '***-**-****'
    END;


-- ============================================================================
-- STEP 3: Apply Masking Policies to Sensitive Columns
-- ============================================================================

-- Apply Masking Policies to the table
ALTER TABLE ANALYTICS.CUSTOMER_SENSITIVE
    MODIFY COLUMN EMAIL SET MASKING POLICY CONTROL.MASK_EMAIL,
    MODIFY COLUMN PHONE_NUMBER SET MASKING POLICY CONTROL.MASK_PHONE,
    MODIFY COLUMN SSN_LAST_9 SET MASKING POLICY CONTROL.MASK_SSN;

-- ============================================================================
-- STEP 4: Test Masking Policies with Different Roles
-- ============================================================================

-- View all masking policies
SHOW MASKING POLICIES;

-- Describe table to see which columns have masking policies
DESCRIBE TABLE ANALYTICS.CUSTOMERS_SENSITIVE;

-- Test as ROLE_ADMIN (sees everything)
USE ROLE TPCH_DEVELOPER;
SELECT * FROM CUSTOMERS_SENSITIVE;

-- Test as ROLE_ANALYST (sees partial data)
USE ROLE TPCH_ANALYST;
SELECT * FROM CUSTOMERS_SENSITIVE ;

-- Test as ROLE_SUPPORT (most data masked)
USE ROLE TPCH_VIEWER;
SELECT * FROM CUSTOMERS_SENSITIVE;
