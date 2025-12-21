-- ============================================================================
-- ## Create SP to Process Stream Data
-- ============================================================================

USE ROLE TPCH_DEVELOPER;
-- ============================================================================
-- Create Stored Procedure for CDC MERGE Logic
-- ============================================================================
-- This procedure processes the stream and merges changes into SILVER table

CREATE OR REPLACE PROCEDURE CONTROL.MERGE_ORDERS_CDC()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_merged NUMBER DEFAULT 0;
BEGIN
    -- Check if stream has data before processing
    LET pending_count NUMBER := (SELECT COUNT(*) FROM CONTROL.STREAM_ORDERS);
    
    IF (pending_count = 0) THEN
        RETURN 'No changes detected in stream. Nothing to process.';
    END IF;
    
    -- MERGE statement: Update existing records or insert new ones
    MERGE INTO ANALYTICS.ORDERS_SILVER AS target
    USING (
        -- Process stream data with transformations
        SELECT 
            O_ORDERKEY,
            O_CUSTKEY,
            O_ORDERSTATUS,

            UDFS.CLASSIFY_ORDERSTATUS(
                O_ORDERSTATUS
            ) AS O_ORDERSTATUS_DESC,
            
            O_TOTALPRICE,
            O_ORDERDATE,
            YEAR(O_ORDERDATE) AS O_ORDER_YEAR,
            MONTH(O_ORDERDATE) AS O_ORDER_MONTH,
            QUARTER(O_ORDERDATE) AS O_ORDER_QUARTER,
            O_ORDERPRIORITY,
            
            UDFS.CLASSIFY_PRIORITY_RANK(
                O_ORDERPRIORITY
            ) AS O_PRIORITY_RANK,    
            
            O_CLERK,
            TRY_CAST(REGEXP_SUBSTR(O_CLERK, '[0-9]+') AS NUMBER) AS O_CLERK_ID,
            O_SHIPPRIORITY,
            O_COMMENT,
            FROM_SOURCE AS SOURCE_FILE,
            CREATED_AT AS FIRST_LOADED_AT,
            METADATA$ACTION,
            METADATA$ISUPDATE
        FROM CONTROL.STREAM_ORDERS
        WHERE METADATA$ACTION IN ('INSERT', 'UPDATE')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY O_ORDERKEY ORDER BY CREATED_AT DESC) = 1
    ) AS source
    ON target.O_ORDERKEY = source.O_ORDERKEY
    
    WHEN MATCHED THEN
        -- Update existing records with latest data
        UPDATE SET
            target.O_CUSTKEY = source.O_CUSTKEY,
            target.O_ORDERSTATUS = source.O_ORDERSTATUS,
            target.O_ORDERSTATUS_DESC = source.O_ORDERSTATUS_DESC,
            target.O_TOTALPRICE = source.O_TOTALPRICE,
            target.O_ORDERDATE = source.O_ORDERDATE,
            target.O_ORDER_YEAR = source.O_ORDER_YEAR,
            target.O_ORDER_MONTH = source.O_ORDER_MONTH,
            target.O_ORDER_QUARTER = source.O_ORDER_QUARTER,
            target.O_ORDERPRIORITY = source.O_ORDERPRIORITY,
            target.O_PRIORITY_RANK = source.O_PRIORITY_RANK,
            target.O_CLERK = source.O_CLERK,
            target.O_CLERK_ID = source.O_CLERK_ID,
            target.O_SHIPPRIORITY = source.O_SHIPPRIORITY,
            target.O_COMMENT = source.O_COMMENT,
            target.SOURCE_FILE = source.SOURCE_FILE,
            target.LAST_UPDATED_AT = CURRENT_TIMESTAMP()
    
    WHEN NOT MATCHED THEN
        -- Insert new records
        INSERT (
            O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_ORDERSTATUS_DESC,
            O_TOTALPRICE, O_ORDERDATE, O_ORDER_YEAR, O_ORDER_MONTH, O_ORDER_QUARTER,
            O_ORDERPRIORITY, O_PRIORITY_RANK, O_CLERK, O_CLERK_ID,
            O_SHIPPRIORITY, O_COMMENT, SOURCE_FILE, FIRST_LOADED_AT, LAST_UPDATED_AT
        )
        VALUES (
            source.O_ORDERKEY, source.O_CUSTKEY, source.O_ORDERSTATUS, source.O_ORDERSTATUS_DESC,
            source.O_TOTALPRICE, source.O_ORDERDATE, source.O_ORDER_YEAR, source.O_ORDER_MONTH, source.O_ORDER_QUARTER,
            source.O_ORDERPRIORITY, source.O_PRIORITY_RANK, source.O_CLERK, source.O_CLERK_ID,
            source.O_SHIPPRIORITY, source.O_COMMENT, source.SOURCE_FILE, source.FIRST_LOADED_AT, CURRENT_TIMESTAMP()
        );
    
    -- Get the number of rows affected
    rows_merged := SQLROWCOUNT;
    
    RETURN 'CDC merge completed. Rows affected: ' || rows_merged || ' (from ' || pending_count || ' stream rows)';
END;
$$;

-- ============================================================================
-- Create SP to Process Customer CDC Report
-- ============================================================================

CREATE OR REPLACE PROCEDURE CONTROL.MERGE_CUSTOMER_CDC()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_merged NUMBER DEFAULT 0;
BEGIN
    -- Check if stream has data before processing
    LET pending_count NUMBER := (SELECT COUNT(*) FROM CONTROL.STREAM_CUSTOMER);
    
    IF (pending_count = 0) THEN
        RETURN 'No changes detected in stream. Nothing to process.';
    END IF;
    
    -- MERGE statement: Update existing records or insert new ones
    MERGE INTO ANALYTICS.CUSTOMER_SILVER AS target
    USING (
        -- Process stream data with transformations
        SELECT 
            C_CUSTKEY,
            C_NAME,
            C_ADDRESS,
            C_NATIONKEY,
            N_NAME AS C_NATION_NAME,
            N_REGIONKEY AS C_REGIONKEY,
            R_NAME AS C_REGION_NAME,
            C_PHONE,
            C_ACCTBAL,
            C_MKTSEGMENT,
            C_COMMENT,            
            
            FROM_SOURCE,
            CREATED_AT,
            METADATA$ACTION,
            METADATA$ISUPDATE
        FROM CONTROL.STREAM_CUSTOMER
        LEFT JOIN STAGING.NATION ON STREAM_CUSTOMER.C_NATIONKEY = NATION.N_NATIONKEY
        LEFT JOIN STAGING.REGION ON REGION.R_REGIONKEY = NATION.N_REGIONKEY

        WHERE METADATA$ACTION IN ('INSERT', 'UPDATE')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY C_CUSTKEY ORDER BY CREATED_AT DESC) = 1
    ) AS source
    ON target.C_CUSTKEY = source.C_CUSTKEY
    
    WHEN MATCHED THEN
        -- Update existing records with latest data
        UPDATE SET
            target.C_NAME = source.C_NAME,
            target.C_ADDRESS = source.C_ADDRESS,
            target.C_NATIONKEY = source.C_NATIONKEY,
            target.C_NATION_NAME = source.C_NATION_NAME,
            target.C_REGIONKEY = source.C_REGIONKEY,
            target.C_REGION_NAME = source.C_REGION_NAME,
            target.C_PHONE = source.C_PHONE,
            target.C_ACCTBAL = source.C_ACCTBAL,
            target.C_MKTSEGMENT = source.C_MKTSEGMENT,
            target.C_COMMENT = source.C_COMMENT,
            target.LOAD_TIMESTAMP = CURRENT_TIMESTAMP()
    
    WHEN NOT MATCHED THEN
        -- Insert new records
        INSERT (
            C_CUSTKEY, 
            C_NAME, 
            C_ADDRESS, 
            C_NATIONKEY, 
            C_NATION_NAME, 
            C_REGIONKEY, 
            C_REGION_NAME, 
            C_PHONE, 
            C_ACCTBAL, 
            C_MKTSEGMENT, 
            C_COMMENT, 
            LOAD_TIMESTAMP
        )
        VALUES (
            source.C_CUSTKEY, 
            source.C_NAME, 
            source.C_ADDRESS, 
            source.C_NATIONKEY, 
            source.C_NATION_NAME, 
            source.C_REGIONKEY, 
            source.C_REGION_NAME, 
            source.C_PHONE, 
            source.C_ACCTBAL, 
            source.C_MKTSEGMENT, 
            source.C_COMMENT, 
            CURRENT_TIMESTAMP()
        );
    
    -- Get the number of rows affected
    rows_merged := SQLROWCOUNT;
    
    RETURN 'CDC merge completed. Rows affected: ' || rows_merged || ' (from ' || pending_count || ' stream rows)';
END;
$$;

-- ============================================================================
-- Create SP to Process LineItem CDC Report
-- ============================================================================

CREATE OR REPLACE PROCEDURE CONTROL.MERGE_LINEITEM_CDC()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_merged NUMBER DEFAULT 0;
BEGIN
    -- Check if stream has data before processing
    LET pending_count NUMBER := (SELECT COUNT(*) FROM CONTROL.STREAM_LINEITEM);
    
    IF (pending_count = 0) THEN
        RETURN 'No changes detected in stream. Nothing to process.';
    END IF;
    
    -- MERGE statement: Update existing records or insert new ones
    MERGE INTO ANALYTICS.LINEITEM_SILVER AS target
    USING (
        -- Process stream data with transformations
        SELECT 
            L_ORDERKEY,
            L_LINENUMBER,
            L_PARTKEY,
            P_NAME AS L_PART_NAME,
            P_TYPE AS L_PART_TYPE,
            L_SUPPKEY,
            S_NAME AS L_SUPPLIER_NAME,
            L_QUANTITY,
            L_EXTENDEDPRICE,
            L_DISCOUNT,
            L_TAX,
            L_RETURNFLAG,
            L_LINESTATUS,
            L_SHIPDATE,
            L_COMMITDATE,
            L_RECEIPTDATE,
            L_SHIPINSTRUCT,
            L_SHIPMODE,
            L_COMMENT,
            
            L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS L_NET_PRICE,
            (L_EXTENDEDPRICE * (1 - L_DISCOUNT)) * (1 + L_TAX) AS L_FINAL_PRICE,
            CASE 
                WHEN DATEDIFF(DAY, L_COMMITDATE, L_RECEIPTDATE) <0 THEN 0 
                ELSE DATEDIFF(DAY, L_COMMITDATE, L_RECEIPTDATE) 
            END AS L_SHIP_DELAY_DAYS,
            
            FROM_SOURCE,
            CREATED_AT,
            METADATA$ACTION,
            METADATA$ISUPDATE
        FROM CONTROL.STREAM_LINEITEM
        LEFT JOIN STAGING.PART ON STREAM_LINEITEM.L_PARTKEY = PART.P_PARTKEY
        LEFT JOIN STAGING.SUPPLIER ON STREAM_LINEITEM.L_SUPPKEY = SUPPLIER.S_SUPPKEY

        WHERE METADATA$ACTION IN ('INSERT', 'UPDATE')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY L_ORDERKEY, L_LINENUMBER ORDER BY CREATED_AT DESC) = 1
    ) AS source
    ON target.L_ORDERKEY = source.L_ORDERKEY
    AND target.L_LINENUMBER = source.L_LINENUMBER
    
    WHEN MATCHED THEN
        -- Update existing records with latest data
        UPDATE SET
			target.L_PARTKEY = source.L_PARTKEY,
			target.L_PART_NAME = source.L_PART_NAME,
			target.L_PART_TYPE = source.L_PART_TYPE,
			target.L_SUPPKEY = source.L_SUPPKEY,
			target.L_SUPPLIER_NAME = source.L_SUPPLIER_NAME,
			target.L_QUANTITY = source.L_QUANTITY,
			target.L_EXTENDEDPRICE = source.L_EXTENDEDPRICE,
			target.L_DISCOUNT = source.L_DISCOUNT,
			target.L_TAX = source.L_TAX,
			target.L_RETURNFLAG = source.L_RETURNFLAG,
			target.L_LINESTATUS = source.L_LINESTATUS,
			target.L_SHIPDATE = source.L_SHIPDATE,
			target.L_COMMITDATE = source.L_COMMITDATE,
			target.L_RECEIPTDATE = source.L_RECEIPTDATE,
			target.L_SHIPINSTRUCT = source.L_SHIPINSTRUCT,
			target.L_SHIPMODE = source.L_SHIPMODE,
			target.L_COMMENT = source.L_COMMENT,
			target.L_NET_PRICE = source.L_NET_PRICE,
			target.L_FINAL_PRICE = source.L_FINAL_PRICE,
			target.L_SHIP_DELAY_DAYS = source.L_SHIP_DELAY_DAYS,
			target.LOAD_TIMESTAMP = CURRENT_TIMESTAMP()
    
    WHEN NOT MATCHED THEN
        -- Insert new records
        INSERT (
			L_ORDERKEY,
			L_LINENUMBER,
			L_PARTKEY,
			L_PART_NAME,
			L_PART_TYPE,
			L_SUPPKEY,
			L_SUPPLIER_NAME,
			L_QUANTITY,
			L_EXTENDEDPRICE,
			L_DISCOUNT,
			L_TAX,
			L_RETURNFLAG,
			L_LINESTATUS,
			L_SHIPDATE,
			L_COMMITDATE,
			L_RECEIPTDATE,
			L_SHIPINSTRUCT,
			L_SHIPMODE,
			L_COMMENT,
			L_NET_PRICE,
			L_FINAL_PRICE,
			L_SHIP_DELAY_DAYS,
			LOAD_TIMESTAMP
        )
        VALUES (
			source.L_ORDERKEY,
			source.L_LINENUMBER,
			source.L_PARTKEY,
			source.L_PART_NAME,
			source.L_PART_TYPE,
			source.L_SUPPKEY,
			source.L_SUPPLIER_NAME,
			source.L_QUANTITY,
			source.L_EXTENDEDPRICE,
			source.L_DISCOUNT,
			source.L_TAX,
			source.L_RETURNFLAG,
			source.L_LINESTATUS,
			source.L_SHIPDATE,
			source.L_COMMITDATE,
			source.L_RECEIPTDATE,
			source.L_SHIPINSTRUCT,
			source.L_SHIPMODE,
			source.L_COMMENT,
			source.L_NET_PRICE,
			source.L_FINAL_PRICE,
			source.L_SHIP_DELAY_DAYS,
			CURRENT_TIMESTAMP()
        );
    
    -- Get the number of rows affected
    rows_merged := SQLROWCOUNT;
    
    RETURN 'CDC merge completed. Rows affected: ' || rows_merged || ' (from ' || pending_count || ' stream rows)';
END;
$$;

-- ============================================================================
-- Create TASK to Automate CDC Pipeline
-- ============================================================================

CREATE OR REPLACE TASK TPCH_ANALYTICS_DB.CONTROL.TASK_CDC_MERGE_ORDERS
    WAREHOUSE = COMPUTE_WH
    -- SCHEDULE = 'USING CRON 05 04 * * * Asia/Ho_Chi_Minh' 
    -- Or use CRON: SCHEDULE = '5 MINUTE'
    AFTER TPCH_ANALYTICS_DB.CONTROL.TASK_REFRESH_PIPE
    WHEN SYSTEM$STREAM_HAS_DATA('STREAM_RAW_ORDERS_LANDING')  -- Only run if stream has data
AS
    CALL CONTROL.MERGE_ORDERS_CDC();

CREATE OR REPLACE TASK TPCH_ANALYTICS_DB.CONTROL.TASK_CDC_MERGE_CUSTOMER
    WAREHOUSE = COMPUTE_WH
    AFTER TPCH_ANALYTICS_DB.CONTROL.TASK_REFRESH_PIPE
    WHEN SYSTEM$STREAM_HAS_DATA('STREAM_RAW_ORDERS_LANDING')  -- Only run if stream has data
AS
    CALL CONTROL.MERGE_CUSTOMER_CDC();

CREATE OR REPLACE TASK TPCH_ANALYTICS_DB.CONTROL.TASK_CDC_MERGE_LINEITEM
    WAREHOUSE = COMPUTE_WH
    AFTER TPCH_ANALYTICS_DB.CONTROL.TASK_REFRESH_PIPE
    WHEN SYSTEM$STREAM_HAS_DATA('STREAM_RAW_ORDERS_LANDING')  -- Only run if stream has data
AS
    CALL CONTROL.MERGE_LINEITEM_CDC();