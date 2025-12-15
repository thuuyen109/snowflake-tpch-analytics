-- ============================================================================
-- SECTION 4: SECURE DATA SHARING
-- ============================================================================
-- Share curated data with external partners/consumers

-- ============================================================================
-- STEP 1: Prepare Data for Sharing
-- ============================================================================

USE ROLE TPCH_DEVELOPER;

-- Create a clean, curated view for sharing (without sensitive data)
CREATE OR REPLACE SECURE VIEW REPORTS.VW_CUSTOMER_PUBLIC AS
SELECT 
    C_CUSTKEY,
    C_NAME,
    C_NATION_NAME,
    C_REGION_NAME,
    C_MKTSEGMENT
FROM ANALYTICS.CUSTOMER_SENSITIVE;
-- Note: Sensitive fields (EMAIL, PHONE, SSN) are excluded

-- Create aggregated view (safe for external sharing)
CREATE OR REPLACE SECURE VIEW REPORTS.VW_CUSTOMER_SUMMARY AS
SELECT 
    C_NATION_NAME,
    C_REGION_NAME,
    C_MKTSEGMENT,
    COUNT(*) AS TOTAL_CUSTOMERS

FROM ANALYTICS.CUSTOMER_SENSITIVE
GROUP BY 1, 2, 3;

-- Verify views
SELECT * FROM REPORTS.VW_CUSTOMER_PUBLIC LIMIT 10;
SELECT * FROM REPORTS.VW_CUSTOMER_SUMMARY LIMIT 10;


-- ============================================================================
-- STEP 2: Create Secure Share
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Create a share (container for sharing objects)
CREATE OR REPLACE SHARE SHARE_CUSTOMER_DATA
    COMMENT = 'Curated customer data for external partners';


-- Grant usage on database to share
GRANT USAGE ON DATABASE TPCH_ANALYTICS_DB TO SHARE SHARE_CUSTOMER_DATA;

-- Grant usage on schema to share
GRANT USAGE ON SCHEMA TPCH_ANALYTICS_DB.REPORTS TO SHARE SHARE_CUSTOMER_DATA;

-- Grant SELECT on views to share (NOT raw tables!)
GRANT SELECT ON VIEW TPCH_ANALYTICS_DB.REPORTS.VW_CUSTOMER_PUBLIC 
    TO SHARE SHARE_CUSTOMER_DATA;

GRANT SELECT ON VIEW TPCH_ANALYTICS_DB.REPORTS.VW_CUSTOMER_SUMMARY 
    TO SHARE SHARE_CUSTOMER_DATA;


-- ============================================================================
-- STEP 3: Manage and Monitor Share
-- ============================================================================

-- View all shares
SHOW SHARES;

/*
share with multiple snowflake accounts example:
ALTER SHARE sales_s ADD ACCOUNTS=<orgname.accountname1>,<orgname.accountname2>;
*/


-- Add consumer account (in production, replace with actual consumer account)
ALTER SHARE SHARE_CUSTOMER_DATA ADD ACCOUNTS = 'ACC_A', 'ACC_B', 'ACC_C';

-- Remove consumer account (if needed)
ALTER SHARE SHARE_CUSTOMER_DATA REMOVE ACCOUNTS = 'ACC_A', 'ACC_B';
