-- Đặt context trước khi tạo UDF (Tuỳ chọn, nhưng nên làm)
USE ROLE TPCH_DEVELOPER;
USE DATABASE TPCH_ANALYTICS_DB;
USE SCHEMA UDFS;

----------------------------------------------------------------------
-- UDF 1: Phân loại khách hàng theo Doanh thu (Customer Segmentation by Revenue)
-- Dùng hàm này để gán phân khúc (Tier) dựa trên tổng doanh thu (total_revenue).
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
-- UDF 2: Validate Phone Number (Sử dụng REGEXP_LIKE)
-- Kiểm tra định dạng số điện thoại (Ví dụ: 10 chữ số, có thể bắt đầu bằng +, v.v.)
-- Ví dụ này kiểm tra định dạng cơ bản: chỉ chứa số, độ dài 10-15 ký tự.
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
-- UDF 3: Validate Email (Sử dụng REGEXP_LIKE)
-- Kiểm tra định dạng email cơ bản: chứa @, không có khoảng trắng, có ít nhất một dấu . sau @
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