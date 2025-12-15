-- ============================================================================
-- # Create Silver (Analytics)
-- ============================================================================


USE SCHEMA TPCH_ANALYTICS_DB.ANALYTICS;
USE ROLE TPCH_DEVELOPER;

-- Silver Table: Orders w clean and enrichment
CREATE OR REPLACE TABLE ORDERS_SILVER (
    O_ORDERKEY          NUMBER(38,0) PRIMARY KEY,
    O_CUSTKEY           NUMBER(38,0),
    O_ORDERSTATUS       VARCHAR(1),
    O_ORDERSTATUS_DESC  VARCHAR(20),          -- Enriched
    O_TOTALPRICE        NUMBER(12,2),
    O_ORDERDATE         DATE,
    O_ORDER_YEAR        NUMBER(4,0),          -- Derived
    O_ORDER_MONTH       NUMBER(2,0),          -- Derived
    O_ORDER_QUARTER     NUMBER(1,0),          -- Derived
    O_ORDERPRIORITY     VARCHAR(15),
    O_PRIORITY_RANK     NUMBER(1,0),          -- Derived
    O_CLERK             VARCHAR(15),
    O_CLERK_ID          NUMBER(9,0),          -- Derived
    O_SHIPPRIORITY      NUMBER(38,0),
    O_COMMENT           VARCHAR(79),
    -- Metadata columns
    SOURCE_FILE         VARCHAR(256),
    FIRST_LOADED_AT     TIMESTAMP_LTZ,
    LAST_UPDATED_AT     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Silver Table: Customers w enrichment
CREATE OR REPLACE TABLE CUSTOMER_SILVER (
    C_CUSTKEY           NUMBER(38,0) PRIMARY KEY,
    C_NAME              VARCHAR(25),
    C_ADDRESS           VARCHAR(40),
    C_NATIONKEY         NUMBER(38,0),
    C_NATION_NAME       VARCHAR(25),          -- Joined from NATION
    C_REGIONKEY         NUMBER(38,0),         -- Joined from NATION->REGION
    C_REGION_NAME       VARCHAR(25),          -- Joined from REGION
    C_PHONE             VARCHAR(15),
    C_ACCTBAL           NUMBER(12,2),
    C_MKTSEGMENT        VARCHAR(10),
    C_COMMENT           VARCHAR(117),
    LOAD_TIMESTAMP      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Silver Table: Lineitem w enrichment
CREATE OR REPLACE TABLE LINEITEM_SILVER (
    L_ORDERKEY          NUMBER(38,0),
    L_LINENUMBER        NUMBER(38,0),
    L_PARTKEY           NUMBER(38,0),
    L_PART_NAME         VARCHAR(55),          -- Joined from PART
    L_PART_TYPE         VARCHAR(25),          -- Joined from PART
    L_SUPPKEY           NUMBER(38,0),
    L_SUPPLIER_NAME     VARCHAR(25),          -- Joined from SUPPLIER
    L_QUANTITY          NUMBER(12,2),
    L_EXTENDEDPRICE     NUMBER(12,2),
    L_DISCOUNT          NUMBER(12,2),
    L_TAX               NUMBER(12,2),
    L_RETURNFLAG        VARCHAR(1),
    L_LINESTATUS        VARCHAR(1),
    L_SHIPDATE          DATE,
    L_COMMITDATE        DATE,
    L_RECEIPTDATE       DATE,
    L_SHIPINSTRUCT      VARCHAR(25),
    L_SHIPMODE          VARCHAR(10),
    L_COMMENT           VARCHAR(44),
    -- Calculated columns
    L_NET_PRICE         NUMBER(12,2),        -- EXTENDEDPRICE * (1 - DISCOUNT)
    L_FINAL_PRICE       NUMBER(12,2),        -- NET_PRICE * (1 + TAX)
    L_SHIP_DELAY_DAYS   NUMBER(38,0),        -- Days between commit and receipt
    LOAD_TIMESTAMP      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)
);


/*
Pricing: Using Clustering Keys will enable Snowflake's Automatic Clustering service. Monitor the Credit costs incurred by this service, especially on large Fact tables like LINEITEM_SILVER.

Order: The order of columns in CLUSTER BY is important. Put the most selective or filter column first.

*/

/*

USE SCHEMA TPCH_ANALYTICS_DB.ANALYTICS;
USE ROLE TPCH_DEVELOPER;

ALTER TABLE ORDERS_SILVER CLUSTER BY (O_ORDERDATE, O_CUSTKEY, O_ORDER_YEAR);
ALTER TABLE CUSTOMER_SILVER CLUSTER BY (C_REGION_NAME, C_NATION_NAME);
ALTER TABLE LINEITEM_SILVER CLUSTER BY (L_SHIPDATE, L_ORDERKEY, L_PARTKEY);

-- monitor cost
SELECT 
    TO_DATE(START_TIME) AS DATE,
    SUM(CREDITS_USED) AS TOTAL_CLUSTERING_CREDITS
FROM 
    SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
GROUP BY 1
ORDER BY 1 DESC;

-- drop cluster
ALTER TABLE ORDERS_SILVER DROP CLUSTERING KEY;
ALTER TABLE CUSTOMER_SILVER DROP CLUSTERING KEY;
ALTER TABLE LINEITEM_SILVER DROP CLUSTERING KEY;

*/

-- ============================================================================
-- # Create Gold (Reports)
-- ============================================================================

USE SCHEMA TPCH_ANALYTICS_DB.REPORTS;
USE ROLE TPCH_DEVELOPER;


-- Gold Table - Daily Sales Summary
CREATE OR REPLACE TABLE DAILY_SALES_SUMMARY (
    SUMMARY_DATE        DATE PRIMARY KEY,
    ORDER_YEAR          NUMBER(4,0),
    ORDER_MONTH         NUMBER(2,0),
    ORDER_QUARTER       NUMBER(1,0),
    TOTAL_ORDERS        NUMBER(38,0),
    TOTAL_CUSTOMERS     NUMBER(38,0),
    TOTAL_REVENUE       NUMBER(15,2),
    AVG_ORDER_VALUE     NUMBER(15,2),
    MIN_ORDER_VALUE     NUMBER(15,2),
    MAX_ORDER_VALUE     NUMBER(15,2),
    LOAD_TIMESTAMP      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Gold Table - Customer Lifetime Value
CREATE OR REPLACE TABLE CUSTOMER_LTV (
    C_CUSTKEY           NUMBER(38,0) PRIMARY KEY,
    C_NAME              VARCHAR(25),
    C_NATION_NAME       VARCHAR(25),
    C_REGION_NAME       VARCHAR(25),
    C_MKTSEGMENT        VARCHAR(10),
    TOTAL_ORDERS        NUMBER(38,0),
    TOTAL_SPENT         NUMBER(15,2),
    AVG_ORDER_VALUE     NUMBER(15,2),
    FIRST_ORDER_DATE    DATE,
    LAST_ORDER_DATE     DATE,
    CUSTOMER_TENURE_DAYS NUMBER(38,0),
    CUSTOMER_TIER       VARCHAR(20),          -- VIP, GOLD, SILVER, BRONZE, STANDARD
    IS_ACTIVE           BOOLEAN,              -- Has order in last 90 days
    LOAD_TIMESTAMP      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);


/*

USE SCHEMA TPCH_ANALYTICS_DB.REPORTS;
USE ROLE TPCH_DEVELOPER;

ALTER TABLE CUSTOMER_LTV CLUSTER BY (C_CUSTKEY, C_REGION_NAME, C_NATION_NAME);
ALTER TABLE DAILY_SALES_SUMMARY CLUSTER BY (SUMMARY_DATE, ORDER_MONTH);

*/
