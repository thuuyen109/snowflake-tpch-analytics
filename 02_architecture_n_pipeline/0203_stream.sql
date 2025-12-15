-- ============================================================================
-- ## Create Streams for Staging Tables
-- ============================================================================

-- Stream captures all changes (INSERT, UPDATE, DELETE) on the source table
-- This is the foundation for incremental processing

CREATE OR REPLACE STREAM CONTROL.STREAM_ORDERS 
ON TABLE STAGING.ORDERS
SHOW_INITIAL_ROWS = FALSE;  -- Set to TRUE if you want to process existing data as changes

CREATE OR REPLACE STREAM CONTROL.STREAM_CUSTOMER
ON TABLE STAGING.CUSTOMER
SHOW_INITIAL_ROWS = FALSE;

CREATE OR REPLACE STREAM CONTROL.STREAM_LINEITEM
ON TABLE STAGING.LINEITEM
SHOW_INITIAL_ROWS = FALSE;
