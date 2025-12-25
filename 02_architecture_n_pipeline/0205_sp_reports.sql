-- ============================================================================
-- ## Create SP to Process Gold (incremental Load from Silver to Gold)
-- ============================================================================

USE ROLE TPCH_DEVELOPER;


CREATE OR REPLACE PROCEDURE CONTROL.LOAD_DAILY_SALES_SUMMARY()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_merged NUMBER;
    max_order_date DATE;
    start_date DATE;

BEGIN
    -- 1. Defind (MAX_ORDER_DATE) on Silver
    SELECT MAX(O_ORDERDATE) INTO max_order_date FROM ANALYTICS.ORDERS_SILVER;
    
    IF (max_order_date IS NULL) THEN
        RETURN 'ORDERS_SILVER is empty. Nothing to process.';
    END IF;

    -- 2. Define start_date from max_date
    start_date := DATEADD(DAY, -7, :max_order_date);

    -- 3. MERGE (UPSERT)
    MERGE INTO REPORTS.DAILY_SALES_SUMMARY AS target
    USING (
        SELECT 
            O_ORDERDATE AS SUMMARY_DATE,
            YEAR(O_ORDERDATE) AS ORDER_YEAR,
            MONTH(O_ORDERDATE) AS ORDER_MONTH,
            QUARTER(O_ORDERDATE) AS ORDER_QUARTER,
            
            COUNT(DISTINCT O_ORDERKEY) AS TOTAL_ORDERS,
            COUNT(DISTINCT O_CUSTKEY) AS TOTAL_CUSTOMERS,
            SUM(O_TOTALPRICE) AS TOTAL_REVENUE,
            AVG(O_TOTALPRICE) AS AVG_ORDER_VALUE,
            MIN(O_TOTALPRICE) AS MIN_ORDER_VALUE,
            MAX(O_TOTALPRICE) AS MAX_ORDER_VALUE
        FROM ANALYTICS.ORDERS_SILVER
        -- Scan within 7 days
        WHERE O_ORDERDATE >= :start_date
        GROUP BY 1, 2, 3, 4
    ) AS source
    ON target.SUMMARY_DATE = source.SUMMARY_DATE

    WHEN MATCHED THEN
        UPDATE SET
            target.TOTAL_ORDERS = source.TOTAL_ORDERS,
            target.TOTAL_CUSTOMERS = source.TOTAL_CUSTOMERS,
            target.TOTAL_REVENUE = source.TOTAL_REVENUE,
            target.AVG_ORDER_VALUE = source.AVG_ORDER_VALUE,
            target.MIN_ORDER_VALUE = source.MIN_ORDER_VALUE,
            target.MAX_ORDER_VALUE = source.MAX_ORDER_VALUE,
            target.LOAD_TIMESTAMP = CURRENT_TIMESTAMP()
            
    WHEN NOT MATCHED THEN
        INSERT (
            SUMMARY_DATE, ORDER_YEAR, ORDER_MONTH, ORDER_QUARTER,
            TOTAL_ORDERS, TOTAL_CUSTOMERS, TOTAL_REVENUE, AVG_ORDER_VALUE,
            MIN_ORDER_VALUE, MAX_ORDER_VALUE, LOAD_TIMESTAMP
        )
        VALUES (
            source.SUMMARY_DATE, source.ORDER_YEAR, source.ORDER_MONTH, source.ORDER_QUARTER,
            source.TOTAL_ORDERS, source.TOTAL_CUSTOMERS, source.TOTAL_REVENUE, source.AVG_ORDER_VALUE,
            source.MIN_ORDER_VALUE, source.MAX_ORDER_VALUE, CURRENT_TIMESTAMP()
        );

    rows_merged := SQLROWCOUNT;
    RETURN 'Daily Sales Summary loaded from ' || :start_date || '. Rows affected: ' || rows_merged;
END;
$$;

-- ============================================================================
-- ## Create SP to Process Customer LTV Report
-- ============================================================================

CREATE OR REPLACE PROCEDURE CONTROL.LOAD_CUSTOMER_LTV()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_merged NUMBER;
    rows_deactivated NUMBER;
    max_order_date DATE;
    start_date DATE;
	now_date DATE;
    customer_list_sql VARCHAR;
    customer_count NUMBER;
    
BEGIN
    -- 1. Define (MAX_ORDER_DATE) in Silver
    SELECT MAX(O_ORDERDATE) INTO max_order_date FROM ANALYTICS.ORDERS_SILVER;
    
    IF (max_order_date IS NULL) THEN
        RETURN 'ORDERS_SILVER is empty. Nothing to process.';
    END IF;
    
    start_date := DATEADD(DAY, -7, :max_order_date);
	
	now_date := DATE('1998-09-02');
	-- change to CURRENT_DATE() on produciton
	-- now_date := CURRENT_DATE();

    
    -- =================================================================
    -- Step 1: Incremental (UPSERT) for Cus just active
    -- =================================================================

    -- Temp table for C_CUSTKEY having orders or updating info  within 7 days
    customer_list_sql := '
        CREATE OR REPLACE TEMPORARY TABLE TEMP_LTV_CUSTOMERS AS
        
		SELECT DISTINCT O_CUSTKEY
        FROM ANALYTICS.ORDERS_SILVER
        WHERE O_ORDERDATE >= ''' || :start_date || '''
		
		UNION 
		SELECT DISTINCT C_CUSTKEY AS O_CUSTKEY
        FROM ANALYTICS.CUSTOMER_SILVER
		WHERE LOAD_TIMESTAMP >= ''' || :start_date || '''::TIMESTAMP
		
		
		;
    ';
    EXECUTE IMMEDIATE customer_list_sql;
    

    IF ((SELECT COUNT(*) FROM TEMP_LTV_CUSTOMERS) > 0) THEN

        MERGE INTO REPORTS.CUSTOMER_LTV AS target
        USING (
            SELECT
                T1.C_CUSTKEY,
                T1.C_NAME,
                T1.C_NATION_NAME,
                T1.C_REGION_NAME,
                T1.C_MKTSEGMENT,
                
                COUNT(DISTINCT T0.O_ORDERKEY) AS TOTAL_ORDERS,
                SUM(T0.O_TOTALPRICE) AS TOTAL_SPENT,
                AVG(T0.O_TOTALPRICE) AS AVG_ORDER_VALUE,
                MIN(T0.O_ORDERDATE) AS FIRST_ORDER_DATE,
                MAX(T0.O_ORDERDATE) AS LAST_ORDER_DATE,
                DATEDIFF(DAY, FIRST_ORDER_DATE, :now_date) AS CUSTOMER_TENURE_DAYS,
                
                -- Logic CUSTOMER_TIER:
                UDFS.CLASSIFY_CUSTOMER_REVENUE(
                    TOTAL_SPENT
                ) AS CUSTOMER_TIER, 
                
                -- Logic IS_ACTIVE: make orders within 90 days
                CASE
                    WHEN LAST_ORDER_DATE >= DATEADD(DAY, -90, :now_date) THEN TRUE
                    ELSE FALSE
                END AS IS_ACTIVE
                
            FROM ANALYTICS.CUSTOMER_SILVER T1 -- root table
            INNER JOIN TEMP_LTV_CUSTOMERS T5 ON T1.C_CUSTKEY = T5.O_CUSTKEY -- only filter cases needed to update
            LEFT JOIN ANALYTICS.ORDERS_SILVER T0 ON T1.C_CUSTKEY = T0.O_CUSTKEY 
    
            GROUP BY 1, 2, 3, 4, 5
        ) AS source
        ON target.C_CUSTKEY = source.C_CUSTKEY
        
        
        WHEN MATCHED THEN
            UPDATE SET
                target.C_NAME = source.C_NAME,
                target.C_NATION_NAME = source.C_NATION_NAME,
                target.C_REGION_NAME = source.C_REGION_NAME,
                target.C_MKTSEGMENT = source.C_MKTSEGMENT,
                target.TOTAL_ORDERS = source.TOTAL_ORDERS,
                target.TOTAL_SPENT = source.TOTAL_SPENT,
                target.AVG_ORDER_VALUE = source.AVG_ORDER_VALUE,
                target.FIRST_ORDER_DATE = source.FIRST_ORDER_DATE,
                target.LAST_ORDER_DATE = source.LAST_ORDER_DATE,
                target.CUSTOMER_TENURE_DAYS = source.CUSTOMER_TENURE_DAYS,
                target.CUSTOMER_TIER = source.CUSTOMER_TIER,
                target.IS_ACTIVE = source.IS_ACTIVE,
                target.LOAD_TIMESTAMP = CURRENT_TIMESTAMP()
                
        -- LTV for new customer
        WHEN NOT MATCHED THEN
            INSERT (
                C_CUSTKEY, C_NAME, C_NATION_NAME, C_REGION_NAME, C_MKTSEGMENT,
                TOTAL_ORDERS, TOTAL_SPENT, AVG_ORDER_VALUE, FIRST_ORDER_DATE,
                LAST_ORDER_DATE, CUSTOMER_TENURE_DAYS, CUSTOMER_TIER, IS_ACTIVE, LOAD_TIMESTAMP
            )
            VALUES (
                source.C_CUSTKEY, source.C_NAME, source.C_NATION_NAME, source.C_REGION_NAME, source.C_MKTSEGMENT,
                source.TOTAL_ORDERS, source.TOTAL_SPENT, source.AVG_ORDER_VALUE, source.FIRST_ORDER_DATE,
                source.LAST_ORDER_DATE, source.CUSTOMER_TENURE_DAYS, source.CUSTOMER_TIER, source.IS_ACTIVE, CURRENT_TIMESTAMP()
            );
    
        rows_merged := SQLROWCOUNT;
    
    END IF;


    -- =================================================================
    -- Step 2: Update deactivation
    -- =================================================================
    
    
    UPDATE REPORTS.CUSTOMER_LTV
    SET 
        IS_ACTIVE = FALSE,
        LOAD_TIMESTAMP = CURRENT_TIMESTAMP()
    WHERE 
        LAST_ORDER_DATE < DATEADD(DAY, -90, :now_date)
        AND IS_ACTIVE = TRUE;
        
    rows_deactivated := SQLROWCOUNT;    

    RETURN 'Customer LTV update completed. ' 
        || 'Incremental Rows affected: ' || rows_merged 
        || '. Deactivated Rows: ' || rows_deactivated
        || ' (Max Order Date: ' || :max_order_date || ')';
END;
$$;

-- ============================================================================
-- ## Create Tasks to Schedule the SPs
-- ============================================================================

CREATE OR REPLACE TASK TPCH_ANALYTICS_DB.CONTROL.TASK_LOAD_CUSTOMER_LTV
    WAREHOUSE = COMPUTE_WH
    -- SCHEDULE = 'USING CRON 05 04 * * * Asia/Ho_Chi_Minh' -- for production
    AFTER TPCH_ANALYTICS_DB.CONTROL.TASK_CDC_MERGE_CUSTOMER, TPCH_ANALYTICS_DB.CONTROL.TASK_CDC_MERGE_ORDERS
AS
    CALL CONTROL.LOAD_CUSTOMER_LTV();


CREATE OR REPLACE TASK TPCH_ANALYTICS_DB.CONTROL.TASK_LOAD_DAILY_SALES_SUMMARY
    WAREHOUSE = COMPUTE_WH
    -- SCHEDULE = 'USING CRON 05 04 * * * Asia/Ho_Chi_Minh' -- for production
    AFTER TPCH_ANALYTICS_DB.CONTROL.TASK_CDC_MERGE_ORDERS
AS
    CALL CONTROL.LOAD_DAILY_SALES_SUMMARY();