USE ROLE TPCH_DEVELOPER;
USE DATABASE TPCH_ANALYTICS_DB;
USE SCHEMA UDFS;

----------------------------------------------------------------------
-- UDF 1: Customer Segmentation by Revenue
-- Use this function to assign a tier based on total revenue.
----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION UDFS.CLASSIFY_CUSTOMER_REVENUE(
    TOTAL_SPENT FLOAT
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    CASE
        WHEN TOTAL_SPENT >= 500000 THEN 'VIP'
        WHEN TOTAL_SPENT >= 100000 THEN 'GOLD'
        WHEN TOTAL_SPENT >= 10000 THEN 'SILVER'
        ELSE 'STANDARD'
    END
$$
;

----------------------------------------------------------------------
-- UDF 2: Validate Phone Number (Using REGEXP_LIKE)
-- Check phone number format (Ex: 10 digits, can start with +, etc.)
-- This example checks the basic format: contains only numbers, 10-15 characters.
----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION UDFS.VALIDATE_PHONE_NUMBER(
    PHONE_NUMBER VARCHAR
)
RETURNS BOOLEAN
AS
$$
    -- REGEXP_LIKE: Kiểm tra chuỗi có khớp với biểu thức chính quy (regex) không
    -- Biểu thức ví dụ: ^[0-9]{10,15}$ (bắt đầu ^, kết thúc $, chỉ chứa các ký tự số {10 đến 15 lần})
    REGEXP_LIKE(TRIM(PHONE_NUMBER), '^[0-9]{10,15}$')
$$
;

----------------------------------------------------------------------
-- UDF 3: Validate Email (Using REGEXP_LIKE)
-- Check basic email formatting: contains @, no spaces, at least one . after @
----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION UDFS.VALIDATE_EMAIL(
    EMAIL VARCHAR
)
RETURNS BOOLEAN
AS
$$
    -- Biểu thức chính quy cơ bản cho email:
    -- ^\S+@\S+\.\S+$
    -- ^\S+: Bắt đầu bằng 1 hoặc nhiều ký tự KHÔNG phải khoảng trắng
    -- @\S+: Ký tự @ theo sau bởi 1 hoặc nhiều ký tự KHÔNG phải khoảng trắng
    -- \.\S+$: Ký tự . theo sau bởi 1 hoặc nhiều ký tự KHÔNG phải khoảng trắng, kết thúc chuỗi
    REGEXP_LIKE(TRIM(EMAIL), '^\\S+@\\S+\\.\\S+$')
$$
;

USE ROLE TPCH_ANALYST;
SELECT
    C_CUSTKEY, 
    C_NAME,
    UDFS.VALIDATE_PHONE_NUMBER(PHONE_NUMBER) AS IS_VALID_PN,
    UDFS.VALIDATE_EMAIL(EMAIL) AS IS_VALID_EMAIL
FROM ANALYTICS.CUSTOMER_SENSITIVE
LIMIT 100;